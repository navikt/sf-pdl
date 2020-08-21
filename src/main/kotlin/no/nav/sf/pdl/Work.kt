package no.nav.sf.pdl

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.prometheus.client.Gauge
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
const val EV_kafkaConsumerTopic = "KAFKA_TOPIC"
const val EV_kafkaSchemaReg = "KAFKA_SCREG"

// Work vault dependencies
const val VAULT_workFilter = "WorkFilter"
const val VAULT_filterEnabled = "FilterEnabled"
const val VAULT_initialLoad = "InitialLoad"
val kafkaSchemaReg = AnEnvironment.getEnvOrDefault(EV_kafkaSchemaReg, "http://localhost:8081")
val kafkaPersonTopic = AnEnvironment.getEnvOrDefault(EV_kafkaProducerTopic, "$PROGNAME-producer")
val kafkaPDLTopic = AnEnvironment.getEnvOrDefault(EV_kafkaConsumerTopic, "$PROGNAME-consumer")

interface ABucket {
    companion object {
        fun getOrDefault(d: String) =
                runCatching { return S3Client.loadFromS3().bufferedReader().readLine() ?: "" }
                        .onSuccess { log.info { "Successfully retrieved value from s3" } }
                        .onFailure { log.error { "Failed to retrieve value from s3" } }
                        .getOrDefault(d)
        fun getFlagOrDefault(b: Boolean) =
                runCatching { return (S3Client.loadFlagFromS3().bufferedReader().readLine() ?: "") == true.toString() }
                        .onSuccess { log.info { "Successfully retrieved flag value from s3" } }
                        .onFailure { log.error { "Failed to retrieve flag value from s3" } }
                        .getOrDefault(b)
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

    val filterEnabled: Boolean = AVault.getSecretOrDefault(VAULT_filterEnabled) == true.toString(),

    val prevFilter: FilterBase = FilterBase.fromS3(),

    val prevEnabled: Boolean = FilterBase.flagFromS3(),

    val initialLoad: Boolean = AVault.getSecretOrDefault(VAULT_initialLoad) == true.toString()
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
    object InvalidCache : ExitReason()
    object Work : ExitReason()
    fun isOK(): Boolean = this is Work || this is NoEvents
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

        fun flagFromS3(): Boolean =
                ABucket.getFlagOrDefault(false)

        fun filterSettingsDiffer(
            currentFilterEnabled: Boolean,
            currentFilter: FilterBase,
            prevFilterEnabled: Boolean,
            prevFilter: FilterBase
        ): Boolean {
            return if (!currentFilterEnabled && !prevFilterEnabled) {
                false // Filter still turned off
            } else if (currentFilterEnabled != prevFilterEnabled) {
                true // Filter has been toggled on or off
            } else {
                currentFilter.hashCode() != prevFilter.hashCode() // Has the filter been changed
            }
        }
    }
}

sealed class Population {
    object Missing : Population()
    object Invalid : Population()

    data class Exist(val map: Map<String, String?>) : Population()

    companion object {
        fun load(kafkaConsumerConfig: Map<String, Any>, topic: String): Population = kotlin.runCatching {
            when (val result = getAllRecords<String, String>(kafkaConsumerConfig, listOf(topic))) {
                is AllRecords.Exist -> {
                    if (result.hasMissingKey())
                        Population.Invalid
                                .also { log.error { "Topic has null in key" } } // Allow null vaule because of Tombestone
                    else {
                        Population.Exist(result.getKeysValues().map { it.k to it.v }.toMap()).also { log.info { "Population size is ${it.map.size}" } }
                    }
                }
                else -> Population.Missing
            }
        }
                .onFailure { log.error { "Error building Population map - ${it.message}" } }
                .getOrDefault(Population.Invalid)
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
                                .also { log.error { "Cache has null in key" } }
                        // Allow null value because of Tombestone
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

@ImplicitReflectionSerializer
internal fun createPersonPair(value: String): Pair<KafkaConsumerStates, PersonBase> {
    return when (val query = value.getQueryFromJson()) {
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

internal fun createTombeStonePair(key: String): Pair<KafkaConsumerStates, PersonBase> {
    val personTombestone = PersonTombestone(aktoerId = key)
    workMetrics.noOfTombestone.inc()
    return Pair(KafkaConsumerStates.IsOk, personTombestone)
}

@ImplicitReflectionSerializer
internal fun initLoad(ws: WorkSettings): ExitReason {
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

    val initTmp = getInitPopulation<String, String>(ws.kafkaConsumerPdl)

    if (initTmp !is InitPopulation.Exist) {
        log.error { "initLoad - could not create init population" }
        return ExitReason.NoFilter
    }

    val initPopulation = (initTmp as InitPopulation.Exist)

    if (!initPopulation.isValid()) {
        log.error { "initLoad - init population invalid" }
        return ExitReason.NoFilter
    }

    workMetrics.noOfKakfaRecordsPdl.inc(initPopulation.records.size.toDouble())
    log.info { "Initial load unique population count : ${initPopulation.records.size}" }

    var exitReason: ExitReason = ExitReason.Work

    AKafkaProducer<ByteArray, ByteArray>(
            config = ws.kafkaProducerPerson
    ).produce {
        initPopulation.records.filter { it.value is PersonTombestone || !filterEnabled || (it.value is PersonSf && personFilter.approved(it.value as PersonSf)) }.map {
            if (it.value is PersonSf) {
                (it.value as PersonSf).toPersonProto()
            } else {
                Pair<PersonProto.PersonKey, PersonProto.PersonValue?>((it.value as PersonTombestone).toPersonTombstoneProtoKey(), null)
            }
        }.fold(true) { acc, pair ->
            acc && pair.second?.let {
                send(kafkaPersonTopic, pair.first.toByteArray(), it.toByteArray()).also { workMetrics.publishedPersons.inc() }
            } ?: sendNullValue(kafkaPersonTopic, pair.first.toByteArray()).also { workMetrics.publishedTombestones.inc() }
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
    return exitReason
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
        log.warn { "No filter for activities, leaving" }
        return Pair(ws, ExitReason.NoFilter)
    }
    val personFilter = ws.filter as FilterBase.Exists
    val filterEnabled = ws.filterEnabled
    log.info { "Continue work with filter enabled: $filterEnabled" }
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
    } else if (tmp is Cache.Invalid) {
        log.error { "Invalid cache, leaving" }
        return Pair(ws, ExitReason.InvalidCache)
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
        } else if (FilterBase.filterSettingsDiffer(ws.filterEnabled, ws.filter, ws.prevEnabled, ws.prevFilter)) {
            if (ws.prevFilter is FilterBase.Missing) {
                log.info { "No filter found in S3 - will consume from beginning of topic" }
            } else {
                log.info { "Difference between filter settings in vault and filter on S3. Vault filter: ${ws.filter} enabled ${ws.filterEnabled}, S3 filter: ${ws.prevFilter as FilterBase.Exists} enabled: ${ws.prevEnabled} - will consume from beginning of topic" }
            }
        } else {
            log.info { "Filter unchanged since last successful work session. Will consume from current offset" }
        }

        val kafkaConsumerPdl = AKafkaConsumer<String, String>(
                config = ws.kafkaConsumerPdl,
                fromBeginning = !ws.initialLoad && (FilterBase.filterSettingsDiffer(ws.filterEnabled, ws.filter, ws.prevEnabled, ws.prevFilter) || cache.isEmpty)
        )
        exitReason = ExitReason.NoKafkaConsumer

        kafkaConsumerPdl.consume { cRecords ->
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
                results.filter { it.second is PersonTombestone || !filterEnabled || (it.second is PersonSf && personFilter.approved(it.second as PersonSf)) }.map {
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

    if (exitReason.isOK()) {
        log.info { "Successful work session finished, will persist filter settings as current cache base" }
        S3Client.persistToS3(json.toJson(FilterBase.Exists.serializer(), personFilter).toString())
        S3Client.persistFlagToS3(filterEnabled)
    } else {
        log.warn { "Failed work session - will not update cache base" }
    }

    return Pair(ws, exitReason)
}
