package no.nav.pdlsf

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.UnstableDefault
import mu.KotlinLogging
import no.nav.pdlsf.proto.PdlSfValuesProto.SfObjectEventKey
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer

private val log = KotlinLogging.logger {}

@OptIn(UnstableDefault::class)
@ImplicitReflectionSerializer
internal fun work(params: Params) {
    log.info { "bootstrap work session starting" }

    val accountCache: MutableMap<String, Int> = mutableMapOf()
    val personCache: MutableMap<String, Int> = mutableMapOf()
/*
    // Get Cachefrom SF topic
    getKafkaConsumerByConfig<ByteArray, ByteArray>(
            mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to params.kafkaBrokers,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArray::class.java,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArray::class.java,
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
            listOf(params.kafkaTopicSf), fromBeginning = true
    ) { cRecords ->
        log.debug { "Start building up Cache of existing SF Objects compaction log" }
        if (!cRecords.isEmpty) {
            cRecords.forEach { record ->
                val aktoerId = SfObjectEventKey.parseFrom(record.key()).aktoerId
                when (SfObjectEventKey.parseFrom(record.key()).sfObjectType) {
                    SfObjectEventKey.SfObjectType.PERSON -> {
                        personCache[aktoerId] = PersonValue.parseFrom(record.value()).hashCode()
                    }
                    SfObjectEventKey.SfObjectType.ACCOUNT -> {
                        accountCache[aktoerId] = AccountValue.parseFrom(record.value()).hashCode()
                    }
                    else -> log.error { "Unknown  Salesforce Object Type in Key" }
                }
            }
            ConsumerStates.IsOkNoCommit
        } else {
            log.info { "Kafka events completed for now - leaving kafka consumer loop" }
            ConsumerStates.IsFinished
        }
    }
    log.debug { "Finished building up Cache of existing SF Objects compaction log size person ${personCache.size} " }
*/

    // Get persons from PDL topic
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
                        Unit // TODO:: Tombestone
                    }
                    else -> if (v.isNotEmpty()) {
                        when (val query = v.getTopicQueryFromJsonString()) {
                            is InvalidTopicQuery -> {
                                log.debug { "InvalidTopicQuery on aktørId- ${cr.key()}" } // TODO :: REMOVE
                                Unit
                            }
                            is TopicQuery -> {
//                                if (query.isAlive) { // && query.inRegion("54")) {
                                when (val queryResponseBase = queryGraphQlSFDetails(cr.key())) {
                                    is QueryErrorResponse -> {
                                        log.debug { "QueryErrorResponse on aktørId - ${cr.key()}" }
                                    } // TODO:: Something  HTTP 200, logisk error fra pdl
                                    is InvalidQueryResponse -> {
                                        log.debug { "InvalidQueryResponse on aktørId - ${cr.key()} " }
                                    } // TODO:: Something Shit hit the fan
                                    is QueryResponse -> {
                                        log.info { "Create protobuf objects" }
                                        val accountKey = createSfObjectEventKey(cr.key(), SfObjectEventKey.SfObjectType.ACCOUNT)
                                        val personKey = createSfObjectEventKey(cr.key(), SfObjectEventKey.SfObjectType.PERSON)

                                        val accountValue = queryResponseBase.toAccountValue()
                                        val personValue = queryResponseBase.toPersonValue()

                                        log.info { "Compare cache to find new and updated persons from pdl" }
                                        if (accountCache.exists(cr.key(), accountValue.hashCode()) != ObjectInCacheStatus.NoChange) {
                                            // TODO :: send to process to topic
                                        }

                                        if (personCache.exists(cr.key(), personValue.hashCode()) != ObjectInCacheStatus.NoChange) {
                                            // TODO :: send to process to topic
                                        }
                                        }
                                    }
                                }
//                                } else {
//                                    Metrics.filterNoHit.inc()
//                                }
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
    }
//    ) {
//        log.info { "Send protobuf SF objects to topic" }
//        personKafkaPayload.forEach { m ->
//            this.send(ProducerRecord(params.kafkaTopicPdl, m.key, m.value))
//        }
//        accountKafkaPayload.forEach { m ->
//            this.send(ProducerRecord(params.kafkaTopicPdl, m.key, m.value))
//        }
//    }
}
