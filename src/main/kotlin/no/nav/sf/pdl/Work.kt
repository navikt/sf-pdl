package no.nav.sf.pdl

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.prometheus.client.Gauge
import java.io.File
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

interface ABucket {
    companion object {
        fun getOrDefault(d: String) =
                runCatching { return S3Client.loadFromS3().bufferedReader().readLine() ?: "" }
                        .onSuccess { log.info { "Successfully retrieved value from s3" } }
                        .onFailure { log.error { "Failed to retrieve value from s3" } }
                        .getOrDefault(d)
    }
}

data class WorkSettings(
    val kafkaConsumerPerson: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java
),
    val kafkaProducerPerson: Map<String, Any> = AKafkaProducer.configBase + mapOf<String, Any>(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java
),
    val kafkaConsumerPdl: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
        "schema.registry.url" to kafkaSchemaReg
),
    val filter: FilterBase = FilterBase.fromJson(AVault.getSecretOrDefault(VAULT_workFilter)),

    val prevFilter: FilterBase = FilterBase.fromS3()
)

data class WMetrics(
    val noOfKakfaRecordsPdl: Gauge = Gauge
            .build()
            .name("no_kafkarecords_pdl_gauge")
            .help("No. of kafka records pdl")
            .register(),

    val noOfTombestone: Gauge = Gauge
            .build()
            .name("no_tombestones")
            .help("No. of kafka records pdl")
            .register(),

    val noOfPersonSf: Gauge = Gauge
            .build()
            .name("no_parsed_persons")
            .help("No. of parsed person sf")
            .register(),

    val sizeOfCache: Gauge = Gauge
            .build()
            .name("size_of_cache")
            .help("Size of person cache")
            .register(),

    val usedAddressTypes: Gauge = Gauge
            .build()
            .name("used_address_gauge")
            .labelNames("type")
            .help("No. of address types used in last work session")
            .register(),
    val publishedPersons: Gauge = Gauge
            .build()
            .name("published_person_gauge")
            .help("No. of persons published to kafka in last work session")
            .register(),
    val publishedTombestones: Gauge = Gauge
            .build()
            .name("published_tombestone_gauge")
            .help("No. of tombestones published to kafka in last work session")
            .register(),
    val cacheIsNewOrUpdated_noKey: Gauge = Gauge
            .build()
            .name("cache_no_key")
            .help("cache no key")
            .register(),
    val cacheIsNewOrUpdated_differentHash: Gauge = Gauge
            .build()
            .name("cache_different_hash")
            .help("cache different hash")
            .register(),
    val cacheIsNewOrUpdated_existing_to_tombestone: Gauge = Gauge
            .build()
            .name("cache_existing_to")
            .help("cache existing to")
            .register(),
    val cacheIsNewOrUpdated_no_blocked: Gauge = Gauge
            .build()
            .name("cache_no_blocked")
            .help("cache no blocked")
            .register(),
    val filterApproved: Gauge = Gauge
            .build()
            .name("filter_approved")
            .help("filter approved")
            .register(),
    val filterDisproved: Gauge = Gauge
            .build()
            .name("filter_disproved")
            .help("filter disproved")
            .register(),
    val consumerIssues: Gauge = Gauge
            .build()
            .name("consumer_issues")
            .help("consumer issues")
            .register(),
    val producerIssues: Gauge = Gauge
            .build()
            .name("producer_issues")
            .help("producer issues")
            .register()
) {
    enum class AddressType {
        VEGAADRESSE, MATRIKKELADRESSE, UKJENTBOSTED, INGEN
    }

    fun clearAll() {
        this.noOfPersonSf.clear()
        this.noOfTombestone.clear()
        this.noOfKakfaRecordsPdl.clear()
        this.sizeOfCache.clear()
        this.usedAddressTypes.clear()
        this.publishedPersons.clear()
        this.publishedTombestones.clear()
        this.cacheIsNewOrUpdated_differentHash.clear()
        this.cacheIsNewOrUpdated_existing_to_tombestone.clear()
        this.cacheIsNewOrUpdated_noKey.clear()
        this.cacheIsNewOrUpdated_no_blocked.clear()
        this.filterApproved.clear()
        this.filterDisproved.clear()
        this.consumerIssues.clear()
        this.producerIssues.clear()
    }
}

val workMetrics = WMetrics()

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
        val regions: List<Region>
    ) : FilterBase() {

        fun approved(p: PersonSf): Boolean {
            val approved = regions.any {
                !p.doed &&
                        it.region == p.region && (it.municipals.isEmpty() || it.municipals.contains(p.kommunenummer))
            }
            if (approved) workMetrics.filterApproved.inc() else workMetrics.filterDisproved.inc()
            return approved
        }
    }

    @Serializable
    data class Region(
        val region: String,
        val municipals: List<String> = emptyList()
    )

    companion object {
        fun fromJson(data: String): FilterBase = runCatching {
            log.info { "Ready to parse filter as json - $data" }
            json.parse(Exists.serializer(), data)
        }
                .onFailure { log.error { "Parsing of person filter in vault failed - ${it.localizedMessage}" } }
                .getOrDefault(Missing)

        fun fromS3(): FilterBase =
                ABucket.getOrDefault("").let { storedFilterJson ->
                    if (storedFilterJson.isEmpty()) {
                        Missing
                    } else {
                        fromJson(storedFilterJson)
                    }
                }
    }
}

/**
 * A minimum cache as a map of persons aktørId and hash code of person details
 */
sealed class Cache {
    object Missing : Cache()
    object Invalid : Cache()

    data class Exist(val map: Map<String, Int?>) : Cache() {

        val isEmpty: Boolean
            get() = map.isEmpty()

        internal fun isNewOrUpdated(item: Pair<PersonProto.PersonKey, PersonProto.PersonValue?>): Boolean = when {
            !map.containsKey(item.first.aktoerId) -> {
                workMetrics.cacheIsNewOrUpdated_noKey.inc()
                true
            }
            item.second != null && map[item.first.aktoerId] != item.second.hashCode() -> {
                workMetrics.cacheIsNewOrUpdated_differentHash.inc()
                true
            } // Updated
            item.second == null && map[item.first.aktoerId] != null -> {
                workMetrics.cacheIsNewOrUpdated_existing_to_tombestone.inc()
                true
            } // Tombestone
            else -> {
                workMetrics.cacheIsNewOrUpdated_no_blocked.inc()
                false
            }
        }
    }

    companion object {
        fun load(kafkaConsumerConfig: Map<String, Any>, topic: String): Cache = kotlin.runCatching {
            when (val result = getAllRecords<ByteArray, ByteArray>(kafkaConsumerConfig, listOf(topic))) {
                is AllRecords.Exist -> {
                    if (result.hasMissingKey())
                        Missing
                                .also { log.error { "Cache has null in key" } } // Allow null vaule because of Tombestone
                    else {
                        Exist(result.getKeysValues().map { it.k.protobufSafeParseKey().aktoerId to it.v.protobufSafeParseValue().hashCode() }.toMap()).also { log.info { "Cache size is ${it.map.size}" } }
                    }
                }
                else -> Missing
            }
        }
                .onFailure { log.error { "Error building Cache - ${it.message}" } }
                .getOrDefault(Invalid)
    }
}

@UnstableDefault
@ImplicitReflectionSerializer
@ExperimentalStdlibApi
internal fun work(ws: WorkSettings): Pair<WorkSettings, ExitReason> {
    log.info { "bootstrap work session starting" }

    workMetrics.clearAll()

    /**
     * Check - no filter means nothing to transfer, leaving
     */
    if (ws.filter is FilterBase.Missing) {
        log.warn { "No active filter for activities, leaving" }
        return Pair(ws, ExitReason.NoFilter)
    }
    val personFilter = ws.filter as FilterBase.Exists
    log.info { "Continue work with filter" }
    /**
     * Check - no cache means no answer for a new event;
     * - is a new activity id
     * - existing activity id, but updated activity details
     * -> leaving
     */
    val tmp = Cache.load(ws.kafkaConsumerPerson, kafkaPersonTopic)
    if (tmp is Cache.Missing) {
        log.error { "Could not read cache, leaving" }
        return Pair(ws, ExitReason.NoCache)
    }

    val cache = tmp as Cache.Exist
    workMetrics.sizeOfCache.set(cache.map.size.toDouble())

    log.info { "Continue work with Cache" }

    var exitReason: ExitReason = ExitReason.NoKafkaProducer
    AKafkaProducer<ByteArray, ByteArray>(
            config = ws.kafkaProducerPerson
    ).produce {

        if (cache.isEmpty) {
            log.info { "Cache is empty - will consume from beginning of pdl topic" }
        } else if (ws.filter.hashCode() != ws.prevFilter.hashCode()) {
            if (ws.prevFilter is FilterBase.Missing) {
                log.info { "No filter found in S3 - will consume from beginning of topic" }
            } else {
                log.info { "Difference between filter in vault and filter on S3. Vault filter: ${ws.filter}, S3 filter: ${ws.prevFilter as FilterBase.Exists} - will consume from beginning of topic" }
            }
        }

        val kafkaConsumerPdl = AKafkaConsumer<String, String>(
                config = ws.kafkaConsumerPdl,
                // Filter change will not trigger until we got S3, only empty cache
                fromBeginning = (ws.filter.hashCode() != ws.prevFilter.hashCode()) || cache.isEmpty
        )
        exitReason = ExitReason.NoKafkaConsumer

        kafkaConsumerPdl.consume { cRecords ->
            log.info { "${cRecords.count()} - consumer records ready to process" }
            workMetrics.noOfKakfaRecordsPdl.inc(cRecords.count().toDouble())
            // leaving if nothing to do
            exitReason = ExitReason.NoEvents
            if (cRecords.isEmpty) return@consume KafkaConsumerStates.IsFinished

            exitReason = ExitReason.Work
            val results = cRecords.map { cr ->
                if (cr.value() == null) {
                    val personTombestone = PersonTombestone(aktoerId = cr.key())
                    workMetrics.noOfTombestone.inc()
                    Pair(KafkaConsumerStates.IsOk, personTombestone)
                } else {
                    // log.info { "debug Consumer record value consumed: ${cr.value()} " }
                    when (val query = cr.value().getQueryFromJson()) {
                        InvalidQuery -> {
                            log.error { "Unable to parse topic value PDL" }
                            Pair(KafkaConsumerStates.HasIssues, PersonInvalid)
                        }
                        is Query -> {
                            when (val personSf = query.toPersonSf()) {
                                is PersonSf -> {
                                    workMetrics.noOfPersonSf.inc()
                                    Pair(KafkaConsumerStates.IsOk, personSf)
                                }
                                is PersonInvalid -> {
                                    Pair(KafkaConsumerStates.HasIssues, PersonInvalid)
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
                // log.info { "${results.size} consumer records resulted in number of Person ${results.filter { it.second is PersonSf }.count()}, Tombestone ${results.filter { it.second is PersonTombestone }.count()}, Invalid  ${results.filter { it.second is PersonInvalid }.count()}" }
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
                }.filter { cache.isNewOrUpdated(it) }
                        .fold(true) { acc, pair ->
                            acc && pair.second?.let {
                                send(kafkaPersonTopic, pair.first.toByteArray(), it.toByteArray()).also { workMetrics.publishedPersons.inc() }
                            } ?: sendNullValue(kafkaPersonTopic, pair.first.toByteArray()).also { workMetrics.publishedTombestones.inc() }
                        }.let { sent ->
                            when (sent) {
                                true -> KafkaConsumerStates.IsOk
                                false -> KafkaConsumerStates.HasIssues.also {
                                    workMetrics.producerIssues.inc()
                                    log.error { "Producer has issues sending to topic" }
                                }
                            }
                        }
            } else {
                workMetrics.consumerIssues.inc()
                KafkaConsumerStates.HasIssues
            }
        } // Consumer pdl topic
    } // Producer person topic

    log.info { "bootstrap work session finished" }

    if (exitReason is ExitReason.Work || exitReason is ExitReason.NoEvents) {
        log.info { "Successful work session finished, will persist filter as current cache base" }
        File("tmp.json").writeText(json.toJson(FilterBase.Exists.serializer(), personFilter).toString())
        S3Client.persistToS3(File("tmp.json"))
    } else {
        log.warn { "Failed work session - will not update cache base" }
    }

    return Pair(ws, exitReason)
}
