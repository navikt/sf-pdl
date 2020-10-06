package no.nav.sf.pdl

import java.io.File
import kotlin.streams.toList
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.KafkaConsumerStates
import no.nav.sf.library.send
import no.nav.sf.library.sendNullValue
import org.apache.kafka.clients.consumer.ConsumerRecord

private val log = KotlinLogging.logger {}

internal fun parsePdlJson(cr: ConsumerRecord<String, String?>): PersonBase {
    if (cr.value() == null) {
        workMetrics.noOfInitialTombestone.inc()
        return PersonTombestone(aktoerId = cr.key())
    } else {
        when (val query = cr.value()?.getQueryFromJson() ?: InvalidQuery) {
            InvalidQuery -> {
                log.error { "Unable to parse topic value PDL" }
                workMetrics.invalidPersonsParsed.inc()
                return PersonInvalid
            }
            is Query -> {
                when (val personSf = query.toPersonSf()) {
                    is PersonSf -> {
                        workMetrics.noOfInitialPersonSf.inc()
                        return personSf
                    }
                    is PersonInvalid -> {
                        workMetrics.invalidPersonsParsed.inc()
                        return PersonInvalid
                    }
                    else -> {
                        log.error { "Unhandled PersonBase from Query.toPersonSf" }
                        workMetrics.invalidPersonsParsed.inc()
                        return PersonInvalid
                    }
                }
            }
        }
    }
}

fun List<Pair<String, PersonBase>>.isValid(): Boolean {
    return filterIsInstance<PersonInvalid>().isEmpty()
}

var heartBeatConsumer: Int = 0

internal fun initLoadTest(ws: WorkSettings) {
    workMetrics.clearAll()
    val kafkaConsumerPdlTest = AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerPdl,
            topics = listOf(kafkaPDLTopic),
            fromBeginning = true
    )

    val resultListTest: MutableList<String> = mutableListOf()

    val a: ArrayList<String?> = arrayListOf() // TODO investigate
    for (i in 0..9) { a.add("") }

    kafkaConsumerPdlTest.consume { cRecords ->
        if (cRecords.isEmpty) return@consume KafkaConsumerStates.IsFinished

        workMetrics.initRecordsParsedTest.inc(cRecords.count().toDouble())
        cRecords.forEach { cr -> resultListTest.add(cr.key()) }
        if (heartBeatConsumer == 0) {
            log.debug { "Investigate Test phase Successfully consumed a batch (This is prompted 100000th consume batch)" }
            if (cRecords.count() > 9) { // TODO investigate
                cRecords.forEachIndexed { index, consumerRecord -> if (index < 10) {
                    a[index] = consumerRecord.value()
                } }
            }
        }
        heartBeatConsumer = ((heartBeatConsumer + 1) % 100000)
        KafkaConsumerStates.IsOk
    }
    heartBeatConsumer = 0

    // TODO investigate
    var msg = "Msgs \n"
    a.forEach { str -> msg += str + "\n" }
    log.info { "Attempt file storage" }
    File("/tmp/investigate").writeText(msg)
    log.info { "File storage done" }

    log.info { "Init test run : Total records from topic: ${resultListTest.size}" }
    workMetrics.initRecordsParsedTest.set(resultListTest.size.toDouble())
    log.info { "Init test run : Total unique records from topic: ${resultListTest.stream().distinct().toList().size}" }
    heartBeatConsumer = 0
}

internal fun initLoad(ws: WorkSettings): ExitReason {
    // initLoadTest(ws)

    workMetrics.clearAll()

    if (ws.filter is FilterBase.Missing) {
        log.warn { "initLoad - No filter for activities, leaving" }
        return ExitReason.NoFilter
    }
    val filter = ws.filter as FilterBase.Exists

    log.info { "Commencing reading all records on topic $kafkaPDLTopic" }

    val resultList: MutableList<Pair<String, PersonBase>> = mutableListOf()

    val kafkaConsumerPdl = AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerPdl,
            topics = listOf(kafkaPDLTopic),
            fromBeginning = true
    )

    kafkaConsumerPdl.consume { cRecords ->
        if (cRecords.isEmpty) return@consume KafkaConsumerStates.IsFinished

        workMetrics.initRecordsParsed.inc(cRecords.count().toDouble())
        cRecords.forEach { cr -> resultList.add(Pair(cr.key(), parsePdlJson(cr))) }
        if (heartBeatConsumer == 0) {
            log.debug { "Successfully consumed a batch (This is prompted 100000th consume batch)" }
        }
        heartBeatConsumer = ((heartBeatConsumer + 1) % 100000)

        KafkaConsumerStates.IsOk
    }

    if (!resultList.isValid()) {
        log.error { "Result contains invalid records" }
        return ExitReason.InvalidCache
    }

    log.info { "Number of records from topic $kafkaPDLTopic is ${resultList.size}" }

    val latestRecords = resultList.toMap() // Remove duplicates

    log.info { "Number of unique aktoersIds (and corresponding messages to handle) on topic $kafkaPDLTopic is ${latestRecords.size}" }

    val numberOfDeadPeopleFound = latestRecords.filter { r -> r.value is PersonSf && (r.value as PersonSf).doed }.size

    log.info { "Number of aktoersIds that is dead people $numberOfDeadPeopleFound" }
    workMetrics.deadPersons.set(numberOfDeadPeopleFound.toDouble())

    val filteredRecords = latestRecords.filter { r -> r.value is PersonTombestone ||
            (r.value is PersonSf &&
                    !(r.value as PersonSf).doed &&
                    (!ws.filterEnabled || filter.approved(r.value as PersonSf, true)))
    }.map {
        if (it.value is PersonSf) {
            (it.value as PersonSf).measureKommune()
            if (it.key == "1000025964669") { log.info { "Found reference person in filtering" } }
            (it.value as PersonSf).toPersonProto()
        } else {
            Pair<PersonProto.PersonKey, PersonProto.PersonValue?>((it.value as PersonTombestone).toPersonTombstoneProtoKey(), null)
        }
    }.toList().asSequence()

    log.info { "Number of records filtered, translated and ready to send ${filteredRecords.count().toDouble()}" }
    workMetrics.noOfInitialKakfaRecordsPdl.set(filteredRecords.count().toDouble())

    var exitReason: ExitReason = ExitReason.NoKafkaProducer

    var produceCount: Int = 0

    filteredRecords.chunked(500000).forEach {
        log.info { "Creating producer for batch ${produceCount++}" }
        AKafkaProducer<ByteArray, ByteArray>(
                config = ws.kafkaProducerPerson
        ).produce {
            it.fold(true) { acc, pair ->
                acc && pair.second?.let {
                    send(kafkaPersonTopic, pair.first.toByteArray(), it.toByteArray()).also { workMetrics.initiallyPublishedPersons.inc() }
                } ?: sendNullValue(kafkaPersonTopic, pair.first.toByteArray()).also { workMetrics.initiallyPublishedTombestones.inc() }
            }.let { sent ->
                when (sent) {
                    true -> exitReason = ExitReason.Work
                    false -> {
                        exitReason = ExitReason.InvalidCache
                        workMetrics.producerIssues.inc()
                        log.error { "Init load - Producer $produceCount has issues sending to topic" }
                    }
                }
            }
        }
    }

    log.info { "Init load - Done with publishing to topic exitReason is Ok? ${exitReason.isOK()} " }

    return exitReason
}

fun PersonSf.measureKommune() {
    val kommuneLabel = if (this.kommunenummer == UKJENT_FRA_PDL) {
        UKJENT_FRA_PDL
    } else {
        PostnummerService.getKommunenummer(this.kommunenummer)?.let {
            it
        } ?: workMetrics.kommune_number_not_found.labels(this.kommunenummer).inc().let { NOT_FOUND_IN_REGISTER }
    }
    workMetrics.kommune.labels(kommuneLabel).inc()
}

fun <T> Sequence<T>.batch(n: Int): Sequence<List<T>> {
    return BatchingSequence(this, n)
}

private class BatchingSequence<T>(val source: Sequence<T>, val batchSize: Int) : Sequence<List<T>> {
    override fun iterator(): Iterator<List<T>> = object : AbstractIterator<List<T>>() {
        val iterate = if (batchSize > 0) source.iterator() else emptyList<T>().iterator()
        override fun computeNext() {
            if (iterate.hasNext()) setNext(iterate.asSequence().take(batchSize).toList())
            else done()
        }
    }
}
