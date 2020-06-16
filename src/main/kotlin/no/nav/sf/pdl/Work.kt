package no.nav.sf.pdl

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.UnstableDefault
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.AVault
import no.nav.sf.library.AllRecords
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.KafkaConsumerStates
import no.nav.sf.library.PROGNAME
import no.nav.sf.library.getAllRecords
import no.nav.sf.library.json
import no.nav.sf.library.send
import no.nav.sf.library.sendNullValue
import no.nav.sf.pdl.FilterBase.Companion.fromJson
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer

private val log = KotlinLogging.logger {}

// Work environment dependencies
const val EV_kafkaProducerTopic = "KAFKA_PRODUCER_TOPIC"
const val EV_kafkaSchemaReg = "KAFKA_SCREG"

// Work vault dependencies
const val VAULT_workFilter = "WorkFilter"
val kafkaSchemaReg = AnEnvironment.getEnvOrDefault(EV_kafkaSchemaReg, "http://localhost:8081")
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
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
    "schema.registry.url" to kafkaSchemaReg
),
    val filter: FilterBase = fromJson(AVault.getSecretOrDefault(VAULT_workFilter)),
    val prevFilter: FilterBase = FilterBase.Missing
)

sealed class ExitReason {
    object NoFilter : ExitReason()
    object NoKafkaProducer : ExitReason()
    object NoKafkaConsumer : ExitReason()
    object NoEvents : ExitReason()
    object NoCache : ExitReason()
    object Work : ExitReason()
}

/**
 * Filter persons
 * Only LIVING persons in listed regions and related municipals will be transferred to Salesforce
 * iff empty list of municipal  - all living persons in that region
 * iff non-empty municipals - only living person in given region AND municipals will be transferred
*/
sealed class FilterBase {
    object Missing : FilterBase()

    @Serializable
    data class Exists(
        val regions: List<RegionBase.Exists>
    ) : FilterBase() {

        fun approved(p: PersonSf): Boolean {
            return regions.any { !p.doed &&
                    it.region == p.region && (it.municipals.isEmpty() || it.municipals.contains(p.kommunenummer)) }
        }
    }

    companion object {
        fun fromJson(data: String): FilterBase = runCatching {
            log.info { "Ready to parse filter as json - $data" }
            json.parse(Exists.serializer(), data)
        }
                .onFailure { log.error { "Parsing of person filter in vault failed - ${it.localizedMessage}" } }
                .getOrDefault(Missing)
    }
}

sealed class RegionBase {

    object Missing : RegionBase()

    @Serializable
    data class Exists(
        val region: String,
        val municipals: List<String> = emptyList()
    ) : RegionBase()
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
     * Check - no filter means nothing to transfer, leaving
     */
    if (ws.filter is FilterBase.Missing) {
        log.warn { "No active filter for activities, leaving" }
        return Pair(ws, ExitReason.NoFilter)
    }
    val personFilter = ws.filter as FilterBase.Exists

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
                fromBeginning = ((ws.filter.hashCode() != ws.prevFilter.hashCode()) || cache.isEmpty)
                        .also { log.info { "Start from beginning due to filter change or empty cache" } }
        )
        exitReason = ExitReason.NoKafkaConsumer

        kafkaConsumerPdl.consume { cRecords ->
            log.info { "${cRecords.count()} - consumer records ready to process" }
            // leaving if nothing to do
            exitReason = ExitReason.NoEvents
            if (cRecords.isEmpty) return@consume KafkaConsumerStates.IsFinished

            exitReason = ExitReason.Work
                val results = cRecords.map { cr ->
                    log.info { "Topic value - ${cr.value()}" }
                    if (cr.value() == null) {
                        log.info { "debug Consumer record value lacking (tombestone)" }
                        val personTombestone = PersonTombestone(aktoerId = cr.key())
                        Pair(KafkaConsumerStates.IsOk, personTombestone)
                    } else {
                        log.info { "debug Consumer record value consumed: ${cr.value()} " }
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
                    results.filter { it.second is PersonTombestone || (it.second is PersonSf && personFilter.approved(it.second as PersonSf)) }.map {
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
