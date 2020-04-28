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
                listOf(params.kafkaTopicPdl), fromBeginning = false // TODO:: false in prod
        ) { cRecords ->
            log.info { "${cRecords.count()} - consumer records ready to process" }
            if (!cRecords.isEmpty) {
                val tombestones = cRecords.filter { it.value() == null }
                val living = cRecords.minus(tombestones).filter { json.parseJson(it.value()).isAlive() }
                val dead = cRecords.minus(living)

                log.info { "${cRecords.count()} - consumer records contains number of living ${living.size}, dead ${dead.size} and ${tombestones.size} tombestone records" }

                val res = runBlocking {
                    val km: MutableMap<ByteArray, ByteArray?> = mutableMapOf()
                    val results = Metrics.graphQlLatency.startTimer().let { rt ->
                    cRecords.map { cr ->
                        async {
                            if (cr.value() == null) {
                                val personTombestone = PersonTombestone(aktoerId = cr.key())
                                Metrics.parsedGrapQLPersons.labels(personTombestone.toMetricsLable()).inc()
                                Pair<ConsumerStates, PersonBase>(ConsumerStates.IsOk, personTombestone)
                            } else if (json.parseJson(cr.value()).isAlive()) {
                                getPersonFromGraphQL(cr.key())
                            } else {
                                Metrics.parsedGrapQLPersons.labels(PersonDead.toMetricsLable()).inc()
                                Pair<ConsumerStates, PersonBase>(ConsumerStates.IsOk, PersonDead)
                            }
                        }
                    }.awaitAll().also { rt.observeDuration() }
                    }

                    val areOk = results.fold(true) { acc, resp -> acc && (resp.first == ConsumerStates.IsOk) }

                    if (areOk) {
                        log.info { "${results.size} consumer records resulted in number of Person ${results.filter { it.second is Person }.count()}, Tombestone ${results.filter { it.second is PersonTombestone }.count()}, Dead  ${results.filter { it.second is PersonDead }.count()}, Unknown ${results.filter { it.second is PersonUnknown }.count()}" }
                        results.forEach { pair ->
                            val personBase = pair.second
                            if (personBase is PersonTombestone) {
                                val personTombstoneProtoKey = personBase.toPersonTombstoneProtoKey()
                                km[personTombstoneProtoKey.toByteArray()] = null
                            } else if (personBase is Person) {
                                val personProto = personBase.toPersonProto()
                                val status = cache.exists(personBase.aktoerId, personProto.second.hashCode())
                                Metrics.publishedPersons.labels(status.name).inc()
                                if (status in listOf(ObjectInCacheStatus.New, ObjectInCacheStatus.Updated)) {
                                    km[personProto.first.toByteArray()] = personProto.second.toByteArray()
                                }
                            }
                        }
                        Pair(ConsumerStates.IsOk, km)
                    } else {
                        Pair(ConsumerStates.HasIssues, km)
                    }
                }

                if (res.first == ConsumerStates.IsOk) {
                    log.info { "${res.second.size} - protobuf Person objects sent to topic ${params.kafkaTopicSf}" }
                    res.second.forEach { m ->
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
