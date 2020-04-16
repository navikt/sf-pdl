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

        val kafkaMessages: MutableMap<ByteArray, ByteArray> = mutableMapOf()

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
            log.debug { "Records polled from PDL compaction log - ${cRecords.count()}" }
            if (!cRecords.isEmpty) {
                var consumerstate: ConsumerStates = ConsumerStates.HasIssues
                cRecords.forEach { cr ->
                    val person: PersonBase = when (val v = cr.value()) {
                        null -> {
                            log.info { "Tombestone" }
                            PersonTombestone(aktoerId = cr.key())
                        }
                        else -> getPersonFromGraphQL(cr.key())
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
                            // Sier det er Ok, men gjør ingenting da dette ikke vil bli løst preprod med det første og er ett Dolly issue
                            // Viktig å fortsette å logge og fange opp dette i prod hvis det er noen cornercases i prod som MFN håndterer feil feks rundt split og merge.
                            ConsumerStates.IsOk
                        }
                        is PersonTombestone -> {
                            Metrics.parsedGrapQLPersons.labels(person.toMetricsLable()).inc()
                            val personTombstoneProto = person.toPersonTombstoneProto()
                            kafkaMessages[personTombstoneProto.first.toByteArray()] = personTombstoneProto.second.toByteArray()
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
                    if (consumerstate != ConsumerStates.IsOk) return@forEach
                }
                consumerstate
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
