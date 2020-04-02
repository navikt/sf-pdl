package no.nav.pdlsf

import io.confluent.kafka.serializers.KafkaAvroDeserializer
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
    // Get Cachefrom SF topic
    val cache = createCache(params)

    log.info { "Finish building up map of persons and accounts protobuf object and send them to Salesforce topic" }
    // Write SF Object to SF topic
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

        // Get persons from PDL topic
        val kafkaMessages: MutableMap<ByteArray, ByteArray> = mutableMapOf()

        getKafkaConsumerByConfig<String, String>(
                mapOf(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to params.kafkaBrokers,
                        "schema.registry.url" to params.kafkaSchemaRegistry,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
                        ConsumerConfig.GROUP_ID_CONFIG to params.kafkaClientID,
                        ConsumerConfig.CLIENT_ID_CONFIG to params.kafkaClientID,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                        // ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 100, // 200 is the maximum batch size accepted by salesforce
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
                ).let { cMap ->
                    if (params.kafkaSecurityEnabled())
                        cMap.addKafkaSecurity(params.kafkaUser, params.kafkaPassword, params.kafkaSecProt, params.kafkaSaslMec)
                    else cMap
                },
                listOf(params.kafkaTopicPdl), fromBeginning = true
        ) { cRecords ->
            log.info { "Start building up map of persons and accounts kafka payload from PDL compaction log, size  - ${cRecords.count()}" }
            if (!cRecords.isEmpty) {
                cRecords.forEach { cr ->
                    when (val v = cr.value()) {
                        null -> {
                            log.info { "Tombestone" }
                            // TODO :: Modellere inn Tombestone
                            Unit
                        }
                        else -> if (v.isNotEmpty()) {
                            when (val queryResponseBase = queryGraphQlSFDetails(cr.key())) {
                                is QueryErrorResponse -> {
                                    log.debug { "QueryErrorResponse on aktørId - ${cr.key()}" }
                                } // TODO:: Something  HTTP 200, logisk error fra pdl
                                is InvalidQueryResponse -> {
                                    log.debug { "InvalidQueryResponse on aktørId - ${cr.key()} " }
                                } // TODO:: Something Shit hit the fan
                                is QueryResponse -> {
                                    log.info { "Compare cache to find new and updated persons from pdl" }
                                    val personProto = queryResponseBase.toPerson().toPersonProto()
                                    if (cache.exists(cr.key(), personProto.second.hashCode()) != ObjectInCacheStatus.NoChange) {
                                        kafkaMessages[personProto.first.toByteArray()] = personProto.second.toByteArray()
                                    }
                                }
                            }
                        }
                    }
                }
                ConsumerStates.IsFinished
            } else {
                log.info { "Kafka events completed for now - leaving kafka consumer loop" }
                ConsumerStates.IsFinished
            }
        }

        log.info { "Send ${kafkaMessages.size} protobuf Person objects to topic ${params.kafkaTopicSf}" }
        kafkaMessages.forEach { m ->
            this.send(ProducerRecord(params.kafkaTopicSf, m.key, m.value))
        }
    }
}
