package no.nav.sf.pdl

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import kotlinx.serialization.Serializable
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

    val initialLoad: Boolean = AVault.getSecretOrDefault(VAULT_initialLoad) == true.toString(),

    val cache: Cache.Exist = Cache.newEmpty(),

    val startUpOffset: Long = -1L
)

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

        fun approved(p: PersonSf, initial: Boolean = false): Boolean {
            val approved = regions.any {
                it.region == p.region && (it.municipals.isEmpty() || it.municipals.contains(p.kommunenummer))
            }
            if (!initial) {
                if (approved) workMetrics.filterApproved.inc() else workMetrics.filterDisproved.inc()
            }
            return approved
        }
    }

    @Serializable
    data class Region(
        val region: String,
        val municipals: List<String> = emptyList()
    )

    companion object {
        fun fromJson(data: String, srcText: String = "vault"): FilterBase = runCatching {
            log.info { "Ready to parse filter from $srcText as json - $data" }
            json.parse(Exists.serializer(), data)
        }
                .onFailure { log.error { "Parsing of person filter from $srcText failed - ${it.localizedMessage}" } }
                .getOrDefault(Missing)

        fun fromS3(): FilterBase =
                ABucket.getOrDefault("").let { storedFilterJson ->
                    if (storedFilterJson.isEmpty()) {
                        Missing
                    } else {
                        fromJson(storedFilterJson, "S3")
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

        internal fun withNewRecords(newRecords: Map<String, Int?>): Exist = Exist(map + newRecords)
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

        fun newEmpty(): Exist = Exist(emptyMap())
    }
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

    val initTmp = getInitPopulation<String, String>(lastDigit, ws.kafkaConsumerPdl, personFilter, filterEnabled)

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

internal fun work(ws: WorkSettings): Triple<WorkSettings, ExitReason, Cache.Exist> {
    log.info { "bootstrap work session starting" }

    workMetrics.clearAll()

    /**
     * Check - no filter means nothing to transfer, leaving
     */
    if (ws.filter is FilterBase.Missing) {
        log.warn { "No filter for activities, leaving" }
        return Triple(ws, ExitReason.NoFilter, ws.cache)
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

    var tmp: Cache
    if (ws.cache.isEmpty) {
        log.info { "No cache in memory will load cache from topic $kafkaPersonTopic" }
        tmp = Cache.load(ws.kafkaConsumerPerson, kafkaPersonTopic)
        if (tmp is Cache.Missing) {
            log.error { "Could not read cache, leaving" }
            return Triple(ws, ExitReason.NoCache, ws.cache)
        } else if (tmp is Cache.Invalid) {
            log.error { "Invalid cache, leaving" }
            return Triple(ws, ExitReason.InvalidCache, ws.cache)
        }
    } else {
        log.info { "Will use cache from previous work run" }
        tmp = ws.cache
    }

    val cache = tmp as Cache.Exist
    val newRecords: MutableMap<String, Int?> = mutableMapOf()

    workMetrics.sizeOfCache.set(cache.map.size.toDouble())
    log.info { "Current cache size is ${cache.map.size} - continue work..." }

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
            log.info { "Filter unchanged since last successful work session. Will consume from current offset $ws." }
        }

        val kafkaConsumerPdl = AKafkaConsumer<String, String>(
                config = ws.kafkaConsumerPdl,
                fromBeginning = false,
                fromOffset = ws.startUpOffset
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
                results.filter { it.second is PersonTombestone || (it.second is PersonSf && !((it.second as PersonSf).doed) && (!filterEnabled || personFilter.approved(it.second as PersonSf))) }.map {
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
                                newRecords[pair.first.aktoerId] = it.hashCode()
                                send(kafkaPersonTopic, pair.first.toByteArray(), it.toByteArray()).also { workMetrics.publishedPersons.inc() }
                                // it.k.protobufSafeParseKey().aktoerId to it.v.protobufSafeParseValue().hashCode()
                                // newRecords[pair.first.aktoerId] = it.hashCode()
                            } ?: sendNullValue(kafkaPersonTopic, pair.first.toByteArray()).also {
                                workMetrics.publishedTombestones.inc()
                            }
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
        return Triple(ws, exitReason, cache.withNewRecords(newRecords))
    } else {
        log.warn { "Failed work session - will not update cache base" }
    }

    return Triple(ws, exitReason, cache)
}
