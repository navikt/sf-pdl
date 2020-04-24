package no.nav.pdlsf

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import java.lang.Exception
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.UnstableDefault
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer

private val log = KotlinLogging.logger {}

class HasKafkaIssues(message: String) : Exception(message)

@OptIn(UnstableDefault::class)
@ImplicitReflectionSerializer
internal fun work(params: Params) {
    log.info { "bootstrap work session starting" }

    val cache = createCache(params)
    if (ServerState.state == ServerStates.KafkaIssues && cache.isEmpty()) {
        log.error { "Terminating work session since cache is empty due to kafka issues" }
        return
    }

    log.info { "Get kafkaproducer to send protobuf person objects to SF topic" }
    getKafkaProducerByConfig<ByteArray, ByteArray>(
            mapOf(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to params.kafkaBrokers,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                    ProducerConfig.ACKS_CONFIG to "all",
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to params.kafkaProducerTimeout,
                    ProducerConfig.CLIENT_ID_CONFIG to params.kafkaClientID
            ).let { map ->
                if (params.kafkaSecurityEnabled())
                    map.addKafkaSecurity(params.kafkaUser, params.kafkaPassword, params.kafkaSecProt, params.kafkaSaslMec)
                else map
            }
    ) {

        log.info { "Start building up map of person from PDL compaction log" }
        getKafkaConsumerByConfig<String, String>(
                mapOf(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to params.kafkaBrokers,
                        "schema.registry.url" to params.kafkaSchemaRegistry,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
                        ConsumerConfig.GROUP_ID_CONFIG to params.kafkaClientID,
                        ConsumerConfig.CLIENT_ID_CONFIG to params.kafkaClientID,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
                ).let { cMap ->
                    if (params.kafkaSecurityEnabled())
                        cMap.addKafkaSecurity(params.kafkaUser, params.kafkaPassword, params.kafkaSecProt, params.kafkaSaslMec)
                    else cMap
                },
                listOf(params.kafkaTopicPdl), fromBeginning = false
        ) { cRecords ->
            log.info { "${cRecords.count()} - consumer records ready to process" }
            if (!cRecords.isEmpty) {
                val toList = cRecords.map { handleConsumerRecord(it) }
                log.info { "Foreach hasIssues - ${toList.filter { it.first == ConsumerStates.HasIssues }.size}" }
                log.info { "Foreach isOk - ${toList.filter { it.first == ConsumerStates.IsOk }.size}" }

                val result = runBlocking {
                    val km: MutableMap<ByteArray, ByteArray?> = mutableMapOf()
                    var hasIssues: Boolean = false
                        cRecords.asFlow()
                                .filterNotNull()
                                .map {
                                    val pair = handleConsumerRecord(it)
                                    if (pair.first != ConsumerStates.IsOk) hasIssues = true
                                    pair
                                }
                                .onEach {
                                    when (val person = it.second) {
                                        is PersonTombestone -> {
                                            log.warn { "In onEach Tombestone" }
                                            val personTombstoneProtoKey = person.toPersonTombstoneProtoKey()
                                            km[personTombstoneProtoKey.toByteArray()] = null
                                        }
                                        is Person -> {
                                            log.warn { "In onEach Person" }
                                            val personProto = person.toPersonProto()
                                            val status = cache.exists(person.aktoerId, personProto.second.hashCode())
                                            log.warn { "In onEach Person status - ${status.name}" }
                                            Metrics.publishedPersons.labels(status.name).inc()
                                            if (status in listOf(ObjectInCacheStatus.New, ObjectInCacheStatus.Updated)) {
                                                km[personProto.first.toByteArray()] = personProto.second.toByteArray()
                                            }
                                        }
                                    }
                                }
                                .collect()
                    if (hasIssues) return@runBlocking Pair(ConsumerStates.HasIssues, km) else return@runBlocking Pair(ConsumerStates.IsOk, km)
                }

                if (result.first == ConsumerStates.IsOk) {
                    log.info { "${result.second.size} - protobuf Person objects sent to topic ${params.kafkaTopicSf}" }
                    result.second.forEach { m ->
                        this.send(ProducerRecord(params.kafkaTopicSf, m.key, m.value))
                    }
                    ConsumerStates.IsOk
                } else {
                    log.error { "Consumerstate issues, is not Ok." }
                    ConsumerStates.HasIssues
                }
            } else {
                log.info { "Kafka events completed for now - leaving kafka consumer loop" }
                ConsumerStates.IsFinished
            }
        }
    }
}

@ImplicitReflectionSerializer
private fun handleConsumerRecord(cr: ConsumerRecord<String, String>): Pair<ConsumerStates, PersonBase> {
    val person = when (cr.value()) {
        null -> {
            PersonTombestone(aktoerId = cr.key())
        }
        else -> {
            val personBase = getPersonFromGraphQL(cr.key())
            personBase
        }
    }
    return when (person) {
        is PersonInvalid -> {
            Metrics.parsedGrapQLPersons.labels(person.toMetricsLable()).inc()
            Pair(ConsumerStates.HasIssues, person)
        }
        is PersonError -> {
            Metrics.parsedGrapQLPersons.labels(person.toMetricsLable()).inc()
            Pair(ConsumerStates.HasIssues, person)
        }
        is PersonUnknown -> {
            Metrics.parsedGrapQLPersons.labels(person.toMetricsLable()).inc()
            Pair(ConsumerStates.HasIssues, person)
        }
        is PersonTombestone -> {
            Metrics.parsedGrapQLPersons.labels(person.toMetricsLable()).inc()
            Pair(ConsumerStates.IsOk, person)
        }
        is Person -> {
            Metrics.parsedGrapQLPersons.labels(person.toMetricsLable()).inc()
            Pair(ConsumerStates.IsOk, person)
        }
    }
}
