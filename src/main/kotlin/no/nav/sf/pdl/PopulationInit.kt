package no.nav.sf.pdl

import java.time.Duration
import java.util.Properties
import mu.KotlinLogging
import no.nav.sf.library.PrestopHook
import no.nav.sf.library.ShutdownHook
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
