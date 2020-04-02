package no.nav.pdlsf

import com.google.protobuf.InvalidProtocolBufferException
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import no.nav.pdlsf.proto.PersonProto.PersonKey
import no.nav.pdlsf.proto.PersonProto.PersonValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer

private val log = KotlinLogging.logger { }

fun createCache(params: Params): Map<String, Int> {
    val cache: MutableMap<String, Int> = mutableMapOf()

    getKafkaConsumerByConfig<ByteArray, ByteArray>(
            mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to params.kafkaBrokers,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                    ConsumerConfig.GROUP_ID_CONFIG to params.kafkaClientID,
                    ConsumerConfig.CLIENT_ID_CONFIG to params.kafkaClientID,
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
            ).let { cMap ->
                if (params.kafkaSecurityEnabled())
                    cMap.addKafkaSecurity(params.kafkaUser, params.kafkaPassword, params.kafkaSecProt, params.kafkaSaslMec)
                else cMap
            },
            listOf(params.kafkaTopicSf), fromBeginning = true
    ) { cRecords ->
        log.info { "Start building up Cache of existing SF Objects compaction log" }
        if (!cRecords.isEmpty) {
            cRecords.forEach { record ->
                val aktoerId = PersonKey.parseFrom(record.key()).aktoerId
                cache[aktoerId] = PersonValue.parseFrom(record.value()).hashCode()
            }
            ConsumerStates.IsOkNoCommit
        } else {
            log.info { "Kafka events completed for now creating cache - leaving kafka consumer loop" }
            ConsumerStates.IsFinished
        }
    }

    log.info { "Finished building up Cache of compaction log size person ${cache.size}" }
    return cache
}

data class Person(
    val aktoerId: String = "",
    val identifikasjonsnummer: String = "",
    val fornavn: String = "",
    val mellomnavn: String = "",
    val etternavn: String = "",
    val adressebeskyttelse: PersonProto.Adressebeskyttelse.Gradering = PersonProto.Adressebeskyttelse.Gradering.UGRADERT,
    val sikkerhetstiltak: List<String> = emptyList(),
    val kommunenummer: String = "",
    val region: String = "",
    val doed: Boolean = false
) {

    fun toPersonProto(): Pair<PersonKey, PersonValue> =
            PersonKey.newBuilder().apply {
                aktoerId = this@Person.aktoerId
            }.build() to PersonValue.newBuilder().apply {
                identifikasjonsnummer = this@Person.identifikasjonsnummer
                fornavn = this@Person.fornavn
                mellomnavn = this@Person.mellomnavn
                etternavn = this@Person.etternavn
                adressebeskyttelse = PersonProto.Adressebeskyttelse.newBuilder().apply {
                    gradering = this@Person.adressebeskyttelse
                }.build()
                this@Person.sikkerhetstiltak.forEach {
                    addSikkerhetstiltak(it)
                }
                // sikkerhetstiltakList. = this@Person.sikkerhetstiltak
                kommunenummer = this@Person.kommunenummer
                region = this@Person.region
                doed = this@Person.doed
            }
                    .build()
}

internal fun ByteArray.protobufSafeParseKey(): PersonKey = this.let { ba ->
    try {
        PersonKey.parseFrom(ba)
    } catch (e: InvalidProtocolBufferException) {
        PersonKey.getDefaultInstance()
    }
}

internal fun ByteArray.protobufSafeParseValue(): PersonValue = this.let { ba ->
    try {
        PersonValue.parseFrom(ba)
    } catch (e: InvalidProtocolBufferException) {
        PersonValue.getDefaultInstance()
    }
}

internal sealed class ObjectInCacheStatus() {
    object New : ObjectInCacheStatus()
    object Updated : ObjectInCacheStatus()
    object NoChange : ObjectInCacheStatus()
}

internal fun Map<String, Int>.exists(aktoerId: String, newValueHash: Int): ObjectInCacheStatus =
        if (!this.containsKey(aktoerId))
            ObjectInCacheStatus.New
        else if ((this.containsKey(aktoerId) && this[aktoerId] != newValueHash))
            ObjectInCacheStatus.Updated
        else
            ObjectInCacheStatus.NoChange
