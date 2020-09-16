package no.nav.sf.pdl

import java.time.Duration
import java.util.Properties
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.PrestopHook
import no.nav.sf.library.ShutdownHook
import no.nav.sf.library.json
import no.nav.sf.library.send
import no.nav.sf.library.sendNullValue
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

private val log = KotlinLogging.logger {}

sealed class InitPopulation {
    object Interrupted : InitPopulation()
    object Failure : InitPopulation()
    data class Exist(val records: Map<String, PersonBase>) : InitPopulation()
}

fun InitPopulation.Exist.isValid(): Boolean {
    return records.values.filterIsInstance<PersonInvalid>().isEmpty()
}

internal fun initLoad(ws: WorkSettings): ExitReason {
    workMetrics.clearAll()
    /**
     * Check - no filter means nothing to transfer, leaving
     */
    if (ws.filter is FilterBase.Missing) {
        log.warn { "initLoad - No filter for activities, leaving" }
        return ExitReason.NoFilter
    }
    val personFilter = ws.filter as FilterBase.Exists
    val filterEnabled = ws.filterEnabled
    log.info { "initLoad - Continue work with filter enabled: $filterEnabled" }

    for (lastDigit in 0..9) {
        log.info { "Commencing pdl topic read for population initialization batch ${lastDigit + 1}/10..." }
        workMetrics.latestInitBatch.set((lastDigit + 1).toDouble())
        val exitReason = initLoadPortion(lastDigit, ws, personFilter, filterEnabled)
        if (exitReason != ExitReason.Work) {
            return exitReason
        }
    }
    log.info { "Successful init session finished, will persist filter settings as current cache base" }
    S3Client.persistToS3(json.toJson(FilterBase.Exists.serializer(), personFilter).toString())
    S3Client.persistFlagToS3(filterEnabled)
    return ExitReason.Work
}

fun initLoadPortion(lastDigit: Int, ws: WorkSettings, personFilter: FilterBase.Exists, filterEnabled: Boolean): ExitReason {

    val initTmp = getInitPopulation<String, String>(lastDigit, ws.kafkaConsumerPdlAlternative, personFilter, filterEnabled)

    if (initTmp !is InitPopulation.Exist) {
        log.error { "initLoad (portion ${lastDigit + 1} of 10) - could not create init population" }
        return ExitReason.NoFilter
    }

    val initPopulation = (initTmp as InitPopulation.Exist)

    if (!initPopulation.isValid()) {
        log.error { "initLoad (portion ${lastDigit + 1} of 10) - init population invalid" }
        return ExitReason.NoFilter
    }

    workMetrics.noOfInitialKakfaRecordsPdl.inc(initPopulation.records.size.toDouble())
    workMetrics.noOfInitialTombestone.inc(initPopulation.records.filter { cr -> cr.value is PersonTombestone }.size.toDouble())
    workMetrics.noOfInitialPersonSf.inc(initPopulation.records.filter { cr -> cr.value is PersonSf }.size.toDouble())
    log.info { "Initial (portion ${lastDigit + 1} of 10) load unique population count : ${initPopulation.records.size}" }

    var exitReason: ExitReason = ExitReason.Work

    AKafkaProducer<ByteArray, ByteArray>(
            config = ws.kafkaProducerPerson
    ).produce {
        initPopulation.records.map { // Assuming initPopulation is already filtered
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
                    log.error { "Init load (portion ${lastDigit + 1} of 10) - Producer has issues sending to topic" }
                }
            }
        }
    }
    return exitReason
}

fun <K, V> getInitPopulation(
    lastDigit: Int,
    config: Map<String, Any>,
    filter: FilterBase.Exists,
    filterEnabled: Boolean,
    topics: List<String> = listOf(kafkaPDLTopic)
): InitPopulation =
        try {
            KafkaConsumer<K, V>(Properties().apply { config.forEach { set(it.key, it.value) } })
                    .apply {
                        this.runCatching {
                            assign(
                                    topics.flatMap { topic ->
                                        partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
                                    }
                            )
                        }.onFailure {
                            log.error { "InitPopulation (portion ${lastDigit + 1} of 10) Failure during topic partition(s) assignment for $topics - ${it.message}" }
                        }
                    }
                    .use { c ->
                        c.runCatching { seekToBeginning(emptyList()) }
                                .onFailure { log.error { "InitPopulation (portion ${lastDigit + 1} of 10) Failure during SeekToBeginning - ${it.message}" } }
                        tailrec fun loop(records: Map<String, PersonBase>): InitPopulation = when {
                            ShutdownHook.isActive() || PrestopHook.isActive() -> InitPopulation.Interrupted
                            else -> {
                                val cr = c.runCatching { Pair(true, poll(Duration.ofMillis(3_000)) as ConsumerRecords<String, String>) }
                                        .onFailure { log.error { "InitPopulation (portion ${lastDigit + 1} of 10) Failure during poll - ${it.localizedMessage}" } }
                                        .getOrDefault(Pair(false, ConsumerRecords<String, String>(emptyMap())))
                                when {
                                    !cr.first -> InitPopulation.Failure
                                    cr.second.isEmpty -> InitPopulation.Exist(records)
                                    // Only deal with messages with key starting with firstDigit (current portion of 10):
                                    else -> loop((records + cr.second.filter { r -> Character.getNumericValue(r.key().last()) == lastDigit }.map {
                                        r ->
                                        workMetrics.initRecordsParsed.inc()
                                        if (r.value() == null) {
                                            val personTombestone = PersonTombestone(aktoerId = r.key())
                                            Pair(r.key(), personTombestone)
                                        } else {
                                            when (val query = r.value().getQueryFromJson()) {
                                                InvalidQuery -> {
                                                    log.error { "InitPopulation (portion ${lastDigit + 1} of 10) Unable to parse topic value PDL" }
                                                    Pair(r.key(), PersonInvalid)
                                                }
                                                is Query -> {
                                                    when (val personSf = query.toPersonSf()) {
                                                        is PersonSf -> {
                                                            Pair(r.key(), personSf)
                                                        }
                                                        is PersonInvalid -> {
                                                            Pair(r.key(), PersonInvalid)
                                                        }
                                                        else -> {
                                                            log.error { "InitPopulation (portion ${lastDigit + 1} of 10) Returned unhandled PersonBase from Query.toPersonSf" }
                                                            Pair(r.key(), PersonInvalid)
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }.filter {
                                        p -> p.second is PersonTombestone || (p.second is PersonSf && !((p.second as PersonSf).doed) && (!filterEnabled || filter.approved(p.second as PersonSf, true)))
                                    }))
                                }
                            }
                        }
                        loop(emptyMap()).also { log.info { "InitPopulation (portion ${lastDigit + 1} of 10) Closing KafkaConsumer" } }
                    }
        } catch (e: Exception) {
            log.error { "InitPopulation (portion ${lastDigit + 1} of 10) Failure during kafka consumer construction - ${e.message}" }
            InitPopulation.Failure
        }

fun <K, V> getStartupOffset(
    config: Map<String, Any>,
    topics: List<String> = listOf(kafkaPDLTopic)
): Long =
        try {
            var startUpOffset = 0L
            KafkaConsumer<K, V>(Properties().apply { config.forEach { set(it.key, it.value) } })
                    .apply {
                        this.runCatching {
                            assign(
                                    topics.flatMap { topic ->
                                        partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
                                    }
                            )
                        }.onFailure {
                            log.error { "Get Startup offset - Failure during topic partition(s) assignment for $topics - ${it.message}" }
                        }
                    }
                    .use { c ->
                        c.runCatching {
                            log.info { "Attempt to register current latest offset as baseline for later" }
                            seekToEnd(emptyList())
                            log.info { "Registered number of assigned partitions ${assignment().size}" }
                            startUpOffset = position(assignment().first())
                            log.info { "startUpOffset registered as $startUpOffset" }
                        }.onFailure { log.error { "Failure during attempt to register current latest offset - ${it.message}" } }
                    }
            startUpOffset
        } catch (e: Exception) {
            log.error { "get Startup Offset Failure during kafka consumer construction - ${e.message}" }
            0L
        }
