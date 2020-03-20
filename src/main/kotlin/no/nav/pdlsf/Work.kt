package no.nav.pdlsf

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.UnstableDefault
import mu.KotlinLogging
import no.nav.pdlsf.proto.PdlSfValuesProto.AccountValue
import no.nav.pdlsf.proto.PdlSfValuesProto.PersonValue
import no.nav.pdlsf.proto.PdlSfValuesProto.SfObjectEventKey
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer

private val log = KotlinLogging.logger {}

@UseExperimental(UnstableDefault::class)
@ImplicitReflectionSerializer
internal fun work(params: Params) {
    log.info { "bootstrap work session starting" }

    val accountKafkaPayload: MutableMap<ByteArray, ByteArray> = mutableMapOf()
    val personKafkaPayload: MutableMap<ByteArray, ByteArray> = mutableMapOf()
    val accountCache: MutableMap<String, Int> = mutableMapOf()
    val personCache: MutableMap<String, Int> = mutableMapOf()

    // Get Cachefrom SF topic
    getKafkaConsumerByConfig<ByteArray, ByteArray>(
            mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to params.kafkaBrokers,
                    "schema.registry.url" to params.kafkaSchemaRegistry,
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
        if (!cRecords.isEmpty) {
            cRecords.forEach { record ->
                val aktoerId = SfObjectEventKey.parseFrom(record.key()).aktoerId
                when (SfObjectEventKey.parseFrom(record.key()).sfObjectType) {
                    SfObjectEventKey.SfObjectType.PERSON -> { personCache[aktoerId] = PersonValue.parseFrom(record.value()).hashCode() }
                    SfObjectEventKey.SfObjectType.ACCOUNT -> { accountCache[aktoerId] = AccountValue.parseFrom(record.value()).hashCode() }
                    else -> log.error { "Unknown  Salesforce Object Type in Key" }
                }
            }
            ConsumerStates.IsOkNoCommit
        } else {
            log.info { "Kafka events completed for now - leaving kafka consumer loop" }
            ConsumerStates.IsFinished
        }
    }

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
        if (!cRecords.isEmpty) {
            cRecords.forEach { cr ->

                when (val v = cr.value()) {
                    null -> Unit // TODO:: Tombestone
                    is String -> if (v.isNotEmpty()) {
                    when (val query = v.getQueryFromJson()) {
                        is InvalidTopicQuery -> Unit
                        is TopicQuery -> {
                            if (query.isAlive && query.inRegion("54")) {
                                log.debug { "Valid Query Object - $query" }
                                when (val res = queryGraphQlSFDetails(cr.key())) {
                                    is QueryErrorResponse -> { } // TODO:: Something  HTTP 200, logisk error
                                    is InvalidQueryResponse -> { } // TODO:: Something Shit hit the fan
                                    is QueryResponse -> {

                                        val accountKey = SfObjectEventKey.newBuilder().apply {
                                            this.aktoerId = cr.key()
                                            this.sfObjectType = SfObjectEventKey.SfObjectType.ACCOUNT
                                        }.build().toByteArray()

                                        val personKey = SfObjectEventKey.newBuilder().apply {
                                            this.aktoerId = cr.key()
                                            this.sfObjectType = SfObjectEventKey.SfObjectType.PERSON
                                        }.build().toByteArray()

                                        val accountValue = AccountValue.newBuilder().apply {
                                            this.identifikasjonsnummer = res.data.hentIdenter?.something // TODO::
                                            this.fornavn = res.data.hentPerson?.navn?.first()?.fornavn
                                            this.mellomnavn = res.data.hentPerson?.navn?.first()?.mellomnavn
                                            this.etternavn = res.data.hentPerson?.navn?.first()?.etternavn
                                        }.build()

                                        val personValue = PersonValue.newBuilder().apply {
                                            this.identifikasjonsnummer = res.data.hentIdenter?.something // TODO::
                                            this.gradering = runCatching { res.data.hentPerson?.adressebeskyttelse?.first()?.gradering?.name }.getOrDefault(Gradering.UGRADERT.name)?.let { PersonValue.Gradering.valueOf(it) }
                                            this.sikkerhetstiltak = res.data.hentPerson?.sikkerhetstiltak?.first()?.beskrivelse
                                            this.kommunenummer = res.data.hentPerson?.bostedsadresse?.findKommunenummer()
                                            this.region = res.data.hentPerson?.bostedsadresse?.findKommunenummer()?.substring(0, 2)
                                        }.build()

                                        // Add new and updated
                                        accountCache[cr.key()].let { hash ->
                                            hash?.let { h -> if (h != accountCache.hashCode()) accountKafkaPayload[accountKey] = accountValue.toByteArray() }
                                                    ?: { accountKafkaPayload[accountKey] = accountValue.toByteArray() }
                                        }
                                        personCache[cr.key()].let { hash ->
                                            hash?.let { h -> if (h != accountCache.hashCode()) personKafkaPayload[personKey] = personValue.toByteArray() }
                                                    ?: { personKafkaPayload[personKey] = personValue.toByteArray() }
                                        }
                                    }
                                }
                            } else {
                                Metrics.filterNoHit.inc()
                            }
                        }
                    }
                }
                }
            }
            ConsumerStates.IsOkNoCommit
        } else {
            log.info { "Kafka events completed for now - leaving kafka consumer loop" }
            ConsumerStates.IsFinished
        }
    }

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
        personKafkaPayload.forEach { m ->
            this.send(ProducerRecord("Topic", m.key, m.value))
        }
        accountKafkaPayload.forEach { m ->
            this.send(ProducerRecord("Topic", m.key, m.value))
        }
    }
}
