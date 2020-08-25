package no.nav.sf.pdl

import java.time.Duration
import java.util.Properties
import kotlinx.serialization.ImplicitReflectionSerializer
import mu.KotlinLogging
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.EV_kafkaTopics
import no.nav.sf.library.PROGNAME
import no.nav.sf.library.PrestopHook
import no.nav.sf.library.ShutdownHook
import no.nav.sf.library.getKafkaTopics
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

@ImplicitReflectionSerializer
fun <K, V> getInitPopulation(
    lastDigit: Int,
    config: Map<String, Any>,
    filter: FilterBase.Exists,
    filterEnabled: Boolean,
    topics: List<String> = AnEnvironment.getEnvOrDefault(EV_kafkaTopics, PROGNAME).getKafkaTopics()
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
                                    else -> loop((records + cr.second.filter { cr -> Character.getNumericValue(cr.key().last()) == lastDigit }.map {
                                        cr ->
                                        workMetrics.initRecordsParsed.inc()
                                        if (cr.value() == null) {
                                            val personTombestone = PersonTombestone(aktoerId = cr.key())
                                            Pair(cr.key(), personTombestone)
                                        } else {
                                            when (val query = cr.value().getQueryFromJson()) {
                                                InvalidQuery -> {
                                                    log.error { "InitPopulation (portion ${lastDigit + 1} of 10) Unable to parse topic value PDL" }
                                                    Pair(cr.key(), PersonInvalid)
                                                }
                                                is Query -> {
                                                    when (val personSf = query.toPersonSf()) {
                                                        is PersonSf -> {
                                                            Pair(cr.key(), personSf)
                                                        }
                                                        is PersonInvalid -> {
                                                            Pair(cr.key(), PersonInvalid)
                                                        }
                                                        else -> {
                                                            log.error { "InitPopulation (portion ${lastDigit + 1} of 10) Returned unhandled PersonBase from Query.toPersonSf" }
                                                            Pair(cr.key(), PersonInvalid)
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }.filter { // Apply filter if enabled
                                        p -> !filterEnabled || p.second is PersonTombestone || (p.second is PersonSf && filter.approved(p.second as PersonSf, true))
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
