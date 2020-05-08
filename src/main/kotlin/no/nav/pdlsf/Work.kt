package no.nav.pdlsf

import io.confluent.kafka.serializers.KafkaAvroDeserializer
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
    if (!ServerState.isOk() && cache.isEmpty()) {
        log.error { "Terminating work session since cache is empty due to kafka issues" }
        return
    }

    log.info { "Get kafkaproducer to send protobuf person objects to SF topic" }
    getKafkaProducerByConfig<ByteArray, ByteArray>(
            mapOf(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to params.envVar.kBrokers,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                    ProducerConfig.ACKS_CONFIG to "all",
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to params.envVar.kProducerTimeout,
                    ProducerConfig.CLIENT_ID_CONFIG to params.envVar.kClientID + "-producer"
            ).let { map ->
                if (params.envVar.kSecurityEnabled)
                    map.addKafkaSecurity(params.vault.kafkaUser, params.vault.kafkaPassword, params.envVar.kSecProt, params.envVar.kSaslMec)
                else map
            }
    ) {

        log.info { "Get KafkaConsumer to start building up map of person from PDL compaction log" }
        getKafkaConsumerByConfig<String, String>(
                mapOf(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to params.envVar.kBrokers,
                        "schema.registry.url" to params.envVar.kSchemaRegistry,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java,
                        ConsumerConfig.GROUP_ID_CONFIG to params.envVar.kClientID,
                        ConsumerConfig.CLIENT_ID_CONFIG to params.envVar.kClientID,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
                ).let { cMap ->
                    if (params.envVar.kSecurityEnabled)
                        cMap.addKafkaSecurity(params.vault.kafkaUser, params.vault.kafkaPassword, params.envVar.kSecProt, params.envVar.kSaslMec)
                    else cMap
                },
                listOf(params.envVar.kTopicPdl), fromBeginning = true // TODO:: false in prod
        ) { cRecords ->
            log.info { "${cRecords.count()} - consumer records ready to process" }
            if (!cRecords.isEmpty) {
                val res = runBlocking {
                    val km: MutableMap<ByteArray, ByteArray?> = mutableMapOf()
                    val results = cRecords.map { cr ->
                        Metrics.noOfKakfaRecordsPdl.inc()
                            if (cr.value() == null) {
                                val personTombestone = PersonTombestone(aktoerId = cr.key())
                                Metrics.parsedGrapQLPersons.labels(personTombestone.toMetricsLable()).inc()
                                Pair<ConsumerStates, PersonBase>(ConsumerStates.IsOk, personTombestone)
                            } else {
                                when (val query = cr.value().getQueryFromJson()) {
                                    is InvalidQuery -> {
                                        log.error { "Unable to parse topic value PDL" }
                                        // Metrics
                                        Pair<ConsumerStates, PersonBase>(ConsumerStates.IsOk, PersonInvalid) // TODO:: HasIssues, ignore when test
                                    }
                                    is Query -> {
                                        when (val personSf = query.toPersonSf()) {
                                            is PersonSf -> {
                                                Pair<ConsumerStates, PersonBase>(ConsumerStates.IsOk, personSf)
                                            }
                                            is PersonInvalid -> {
                                                Pair<ConsumerStates, PersonBase>(ConsumerStates.IsOk, PersonInvalid) // TODO:: HasIssues, ignore when test
                                            }
                                            else -> {
                                                log.error { "Returned unhandled PersonBase from Query.toPersonSf" }
                                                Pair<ConsumerStates, PersonBase>(ConsumerStates.HasIssues, PersonInvalid)
                                            }
                                        }
                                    }
                                }
                            }
                    }

                    val areOk = results.fold(true) { acc, resp -> acc && (resp.first == ConsumerStates.IsOk) }

                    if (areOk) {
                        log.info { "${results.size} consumer records resulted in number of Person ${results.filter { it.second is PersonSf }.count()}, Tombestone ${results.filter { it.second is PersonTombestone }.count()}, Invalid  ${results.filter { it.second is PersonInvalid }.count()}" }
                        results.forEach { pair ->
                            val personBase = pair.second
                            if (personBase is PersonTombestone) {
                                val personTombstoneProtoKey = personBase.toPersonTombstoneProtoKey()
                                km[personTombstoneProtoKey.toByteArray()] = null
                            } else if (personBase is PersonSf) {
                                val personProto = personBase.toPersonProto()
                                val status = cache.exists(personBase.aktoerId, personProto.second.hashCode())
                                Metrics.publishedPersons.labels(status.name).inc()
                                if (status in listOf(ObjectInCacheStatus.New, ObjectInCacheStatus.Updated)) {
                                    km[personProto.first.toByteArray()] = personProto.second.toByteArray()
                                }
                            }
                        }
                        Pair(ConsumerStates.IsOkNoCommit, km) // TODO:: Prod IsOK
                    } else {
                        Pair(ConsumerStates.HasIssues, km)
                    }
                }

                if (res.first == ConsumerStates.IsOk) {
                    log.info { "${res.second.size} - protobuf Person objects sent to topic ${params.envVar.kTopicSf}" }
                    res.second.forEach { m ->
                        this.send(ProducerRecord(params.envVar.kTopicSf, m.key, m.value))
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
