package no.nav.sf.pdl

import com.google.protobuf.InvalidProtocolBufferException
import kotlinx.serialization.json.JsonElement
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto.PersonKey
import no.nav.pdlsf.proto.PersonProto.PersonValue

private val log = KotlinLogging.logger { }

// fun createCache(ws: WorkSettings): Map<String, Int?> {
//    val cache: MutableMap<String, Int?> = mutableMapOf()
//
//    getKafkaConsumerByConfig<ByteArray, ByteArray>(
//            mapOf(
//                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to params.envVar.kBrokers,
//                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
//                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
//                    ConsumerConfig.GROUP_ID_CONFIG to params.envVar.kClientID,
//                    ConsumerConfig.CLIENT_ID_CONFIG to params.envVar.kClientID,
//                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
//            ).let { cMap ->
//                if (params.envVar.kSecurityEnabled)
//                    cMap.addKafkaSecurity(params.vault.kafkaUser, params.vault.kafkaPassword, params.envVar.kSecProt, params.envVar.kSaslMec)
//                else cMap
//            },
//            listOf(params.envVar.kTopicSf), fromBeginning = true
//    ) { cRecords ->
//        if (!cRecords.isEmpty) {
//            cRecords.forEach { record ->
//                val aktoerId = record.key().protobufSafeParseKey().aktoerId
//                if (record.value() == null) {
//                    cache[aktoerId] = null
//                } else {
//                    cache[aktoerId] = record.value().protobufSafeParseValue().hashCode()
//                }
//            }
//            ConsumerStates.IsOkNoCommit
//        } else {
//            log.info { "Kafka events completed for now creating cache - leaving kafka consumer loop" }
//            ConsumerStates.IsFinished
//        }
//    }
//    Metrics.cachedPersons.inc(cache.size.toDouble())
//    log.info { "Finished building up Cache of compaction log size person ${cache.size}" }
//
//    return cache
// }

fun JsonElement.isAlive(): Boolean = runCatching {
    jsonObject.content["hentPerson"]?.let { hi ->
        hi.jsonObject.content["doedsfall"]?.let {
            it.jsonArray.isNullOrEmpty()
        } ?: true
    } ?: true
}
.onFailure { log.info { "Failure resolving if person is Alive - ${it.localizedMessage}" } }
.getOrDefault(true)

sealed class PersonBase

object PersonInvalid : PersonBase()

data class PersonTombestone(
    val aktoerId: String
) : PersonBase() {
    fun toPersonTombstoneProtoKey(): PersonKey =
            PersonKey.newBuilder().apply {
                aktoerId = this@PersonTombestone.aktoerId
            }.build()
}

data class PersonSf(
    val aktoerId: String = "",
    val identifikasjonsnummer: String = "",
    val fornavn: String = "",
    val mellomnavn: String = "",
    val etternavn: String = "",
    val adressebeskyttelse: AdressebeskyttelseGradering = AdressebeskyttelseGradering.UGRADERT,
    val sikkerhetstiltak: List<String> = emptyList(),
    val kommunenummer: String = "",
    val region: String = "",
    val doed: Boolean = false
) : PersonBase() {

    fun toPersonProto(): Pair<PersonKey, PersonValue> =
            PersonKey.newBuilder().apply {
                aktoerId = this@PersonSf.aktoerId
            }.build() to PersonValue.newBuilder().apply {
                identifikasjonsnummer = this@PersonSf.identifikasjonsnummer
                fornavn = this@PersonSf.fornavn
                mellomnavn = this@PersonSf.mellomnavn
                etternavn = this@PersonSf.etternavn
                adressebeskyttelse = PersonValue.Gradering.valueOf(this@PersonSf.adressebeskyttelse.name)
                this@PersonSf.sikkerhetstiltak.forEach {
                    addSikkerhetstiltak(it)
                }
                kommunenummer = this@PersonSf.kommunenummer
                region = this@PersonSf.region
                doed = this@PersonSf.doed
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
