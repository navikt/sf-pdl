package no.nav.pdlsf

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.UnstableDefault
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer

private val log = KotlinLogging.logger {}

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
                listOf(params.kafkaTopicPdl), fromBeginning = true
        ) { cRecords ->
            log.info { "${cRecords.count()} - consumer records ready to process" }
            val kafkaMessages: MutableMap<ByteArray, ByteArray?> = mutableMapOf()
            if (!cRecords.isEmpty) {
                var consumerstate: ConsumerStates = ConsumerStates.HasIssues
                val listConsumerStates = runBlocking {
                    cRecords.map { cr ->
                        async {
                            val person = when (cr.value()) {
                                null -> {
                                    PersonTombestone(aktoerId = cr.key())
                                }
                                else -> {
                                    val personBase = getPersonFromGraphQL(cr.key())
                                    personBase
                                }
                            }
                            consumerstate = when (person) {
                                is PersonInvalid -> {
                                    Metrics.parsedGrapQLPersons.labels(person.toMetricsLable()).inc()
                                    ConsumerStates.HasIssues
                                }
                                is PersonError -> {
                                    Metrics.parsedGrapQLPersons.labels(person.toMetricsLable()).inc()
                                    ConsumerStates.HasIssues
                                }
                                is PersonUnknown -> {
                                    Metrics.parsedGrapQLPersons.labels(person.toMetricsLable()).inc()
                                    ConsumerStates.HasIssues
                                }
                                is PersonTombestone -> {
                                    Metrics.parsedGrapQLPersons.labels(person.toMetricsLable()).inc()
                                    val personTombstoneProtoKey = person.toPersonTombstoneProtoKey()
                                    kafkaMessages[personTombstoneProtoKey.toByteArray()] = null
                                    ConsumerStates.IsOk
                                }
                                is Person -> {
                                    Metrics.parsedGrapQLPersons.labels(person.toMetricsLable()).inc()
                                    val personProto = person.toPersonProto()
                                    val status = cache.exists(cr.key(), personProto.second.hashCode())
                                    Metrics.publishedPersons.labels(status.name).inc()
                                    if (status in listOf(ObjectInCacheStatus.New, ObjectInCacheStatus.Updated)) {
                                        kafkaMessages[personProto.first.toByteArray()] = personProto.second.toByteArray()
                                    }
                                    ConsumerStates.IsOk
                                }
                            }
                            consumerstate
                        }
                    }.awaitAll()
                }

                if (listConsumerStates.all { it != ConsumerStates.IsOk }) {
                    log.warn { "Consumerstate issues, is not Ok. Return from foreach  with consumerstate" }
                    return@getKafkaConsumerByConfig ConsumerStates.HasIssues
                } else {
                    log.info { "${kafkaMessages.size} - protobuf Person objects sent to topic ${params.kafkaTopicSf}" }
                    kafkaMessages.forEach { m ->
                        this.send(ProducerRecord(params.kafkaTopicSf, m.key, m.value))
                    }
                    kafkaMessages.clear()
                    ConsumerStates.IsOk
                }
            } else {
                log.info { "Kafka events completed for now - leaving kafka consumer loop" }
                ConsumerStates.IsFinished
            }
        }
    }
}
