package no.nav.sf.pdl

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
internal fun initLoad(ws: WorkSettings): ExitReason {
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
            log.debug { "Successfully consumed a batch (This is prompted 1000th consume batch)" }
        }
        heartBeatConsumer = ((heartBeatConsumer + 1) % 1000)

        KafkaConsumerStates.IsOk
    }

    if (!resultList.isValid()) {
        log.error { "Result contains invalid records" }
        return ExitReason.InvalidCache
    }

    log.info { "Number of records from topic $kafkaPDLTopic is ${resultList.size}" }

    val latestRecords = resultList.toMap()

    log.info { "Number of unique aktoersIds (and corresponding messages to handle) on topic $kafkaPDLTopic is ${latestRecords.size}" }

    val numberOfDeadPeopleFound = latestRecords.filter { r -> r.value is PersonSf && (r.value as PersonSf).doed }.size

    log.info { "Number of aktoersIds that is dead people $numberOfDeadPeopleFound" }
    workMetrics.deadPersons.set(numberOfDeadPeopleFound.toDouble())

    val filteredRecords = latestRecords.filter { r -> r.value is PersonTombestone ||
            (r.value is PersonSf &&
                    !(r.value as PersonSf).doed &&
                    (!ws.filterEnabled || filter.approved(r.value as PersonSf, true))) }

    log.info { "Do we see the reference person? ${filteredRecords.containsKey("1000025964669")}" }

    log.info { "Number of records filtered and ready to send ${filteredRecords.size.toDouble()}" }
    workMetrics.noOfInitialKakfaRecordsPdl.set(filteredRecords.size.toDouble())

    var exitReason: ExitReason = ExitReason.NoKafkaProducer

    AKafkaProducer<ByteArray, ByteArray>(
            config = ws.kafkaProducerPerson
    ).produce {
        filteredRecords.map {
            if (it.value is PersonSf) {
                (it.value as PersonSf).toPersonProto()
            } else {
                Pair<PersonProto.PersonKey, PersonProto.PersonValue?>((it.value as PersonTombestone).toPersonTombstoneProtoKey(), null)
            }
        }.fold(true) { acc, pair ->
            acc && pair.second?.let {
                send(kafkaPersonTopic, pair.first.toByteArray(), it.toByteArray()).also { workMetrics.initiallyPublishedPersons.inc() }
            } ?: sendNullValue(kafkaPersonTopic, pair.first.toByteArray()).also { workMetrics.initiallyPublishedTombestones.inc() }
        }.let { sent ->
            when (sent) {
                true -> exitReason = ExitReason.Work
                false -> {
                    exitReason = ExitReason.InvalidCache
                    workMetrics.producerIssues.inc()
                    log.error { "Init load - Producer has issues sending to topic" }
                }
            }
        }
    }

    if (exitReason.isOK()) {
        // Let´s take a look on the persons and give us some metrics on the kommune in the parsed data
        filteredRecords.filter { cr -> cr.value is PersonSf }.forEach { cr ->
            val kommuneLabel = if ((cr.value as PersonSf).kommunenummer == UKJENT_FRA_PDL) {
                UKJENT_FRA_PDL
            } else {
                PostnummerService.getPostnummer((cr.value as PersonSf).kommunenummer)?.let {
                    it.kommune
                } ?: workMetrics.kommune_number_not_found.labels((cr.value as PersonSf).kommunenummer).inc().let { NOT_FOUND_IN_REGISTER }
            }
            workMetrics.kommune.labels(kommuneLabel).inc()
        }
    }

    return exitReason
}
