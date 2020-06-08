package no.nav.sf.pdl

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.UnstableDefault
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.AllRecords
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.KafkaConsumerStates
import no.nav.sf.library.PROGNAME
import no.nav.sf.library.getAllRecords
import no.nav.sf.library.send
import no.nav.sf.library.sendNullValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer

private val log = KotlinLogging.logger {}

// Work environment dependencies
const val EV_kafkaProducerTopic = "KAFKA_PRODUCER_TOPIC"
val kafkaPersonTopic = AnEnvironment.getEnvOrDefault(EV_kafkaProducerTopic, "$PROGNAME-producer")

data class WorkSettings(
    val kafkaConsumerPerson: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java
    ),
    val kafkaProducerPerson: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java
    ),
    val kafkaConsumerPdl: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java
    )
)

sealed class ExitReason {
    object NoKafkaProducer : ExitReason()
    object NoKafkaConsumer : ExitReason()
    object NoEvents : ExitReason()
    object NoCache : ExitReason()
    object Work : ExitReason()
}

/**
 * A minimum cache PersonCache as a map of persons aktørId and hash code of person details
 */
sealed class Cache {
    object Missing : Cache()

    data class Exist(val map: Map<String, Int>) : Cache() {

        val isEmpty: Boolean
            get() = map.isEmpty()

        internal fun isNewOrUpdated(item: Pair<PersonProto.PersonKey, PersonProto.PersonValue?>): Boolean = when {
            !map.containsKey(item.first.aktoerId) -> true
            map.containsKey(item.first.aktoerId) && map[item.first.aktoerId] != item.second.hashCode() -> true
            map.containsKey(item.first.aktoerId) && item.second == null -> true // Tombestone
            else -> false
        }
    }

    companion object {
        fun load(kafkaConsumerConfig: Map<String, Any>, topic: String): Cache =
                when (val result = getAllRecords<String, String>(kafkaConsumerConfig, listOf(topic))) {
                    is AllRecords.Exist -> {
                        Exist(result.getKeysValues().map { it.k to it.v.hashCode() }.toMap())
                    }
                    else -> Missing
                }
    }
}

@UnstableDefault
@ImplicitReflectionSerializer
@ExperimentalStdlibApi
internal fun work(ws: WorkSettings): Pair<WorkSettings, ExitReason> {

    log.info { "bootstrap work session starting" }

    /**
     * Check - no cache means no answer for a new event;
     * - is a new activity id
     * - existing activity id, but updated activity details
     * -> leaving
     */
    val tmp = Cache.load(ws.kafkaConsumerPerson, kafkaPersonTopic)
    if (tmp is Cache.Missing) {
        log.error { "Could not read activity cache, leaving" }
        return Pair(ws, ExitReason.NoCache)
    }

    val cache = tmp as Cache.Exist

    var exitReason: ExitReason = ExitReason.NoKafkaProducer
    AKafkaProducer<ByteArray, ByteArray>(
            config = ws.kafkaProducerPerson
    ).produce {

        val kafkaConsumerPdl = AKafkaConsumer<String, String>(
                config = ws.kafkaConsumerPdl,
                fromBeginning = false
        )
        exitReason = ExitReason.NoKafkaConsumer

        kafkaConsumerPdl.consume { cRecords ->
            log.info { "${cRecords.count()} - consumer records ready to process" }
            // leaving if nothing to do
            exitReason = ExitReason.NoEvents
            if (cRecords.isEmpty) return@consume KafkaConsumerStates.IsFinished

            exitReason = ExitReason.Work
                val results = cRecords.map { cr ->
                    if (cr.value() == null) {
                        val personTombestone = PersonTombestone(aktoerId = cr.key())
                        Pair(KafkaConsumerStates.IsOk, personTombestone)
                    } else {
                        when (val query = cr.value().getQueryFromJson()) {
                            InvalidQuery -> {
                                log.error { "Unable to parse topic value PDL" }
                                Pair(KafkaConsumerStates.HasIssues, PersonInvalid) // TODO:: HasIssues, ignore when test
                            }
                            is Query -> {
                                when (val personSf = query.toPersonSf()) {
                                    is PersonSf -> {
                                        Pair(KafkaConsumerStates.IsOk, personSf)
                                    }
                                    is PersonInvalid -> {
                                        Pair(KafkaConsumerStates.HasIssues, PersonInvalid) // TODO:: HasIssues, ignore when test
                                    }
                                    else -> {
                                        log.error { "Returned unhandled PersonBase from Query.toPersonSf" }
                                        Pair(KafkaConsumerStates.HasIssues, PersonInvalid)
                                    }
                                }
                            }
                        }
                    }
                }
                val areOk = results.fold(true) { acc, resp -> acc && (resp.first == KafkaConsumerStates.IsOk) }

                if (areOk) {
                    log.info { "${results.size} consumer records resulted in number of Person ${results.filter { it.second is PersonSf }.count()}, Tombestone ${results.filter { it.second is PersonTombestone }.count()}, Invalid  ${results.filter { it.second is PersonInvalid }.count()}" }
                    results.map {
                        when (val personBase = it.second) {
                            is PersonTombestone -> {
                                Pair<PersonProto.PersonKey, PersonProto.PersonValue?>(personBase.toPersonTombstoneProtoKey(), null)
                            }
                            is PersonSf -> {
                                personBase.toPersonProto()
                            }
                            else -> return@consume KafkaConsumerStates.HasIssues
                        }
                    }.filter { cache.isNewOrUpdated(it) }.fold(true) { acc, pair -> acc && pair.second?.let { send(kafkaPersonTopic, pair.first.toByteArray(), it.toByteArray()) } ?: sendNullValue(kafkaPersonTopic, pair.first.toByteArray()) }
                    KafkaConsumerStates.IsOk
                } else {
                    KafkaConsumerStates.HasIssues
                }
        } // Consumer pdl topic
    } // Producer person topic


    log.info { "bootstrap work session finished" }
    return Pair(ws, exitReason)
}
