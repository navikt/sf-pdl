package no.nav.pdlsf

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.KSerializer
import kotlinx.serialization.PrimitiveDescriptor
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor
import kotlinx.serialization.Serializable
import kotlinx.serialization.UnstableDefault
import mu.KotlinLogging
import no.nav.pdlsf.proto.PdlSfValuesProto

private val log = KotlinLogging.logger { }

object IsoLocalDateSerializer : LocalDateSerializer(DateTimeFormatter.ISO_LOCAL_DATE)

open class LocalDateSerializer(private val formatter: DateTimeFormatter) : KSerializer<LocalDate> {
    override val descriptor: SerialDescriptor = PrimitiveDescriptor("LocalDate", PrimitiveKind.STRING) // StringDescriptor.withName("java.time.LocalDate")
    override fun deserialize(decoder: Decoder): LocalDate {
        return LocalDate.parse(decoder.decodeString(), formatter)
    }

    override fun serialize(encoder: Encoder, obj: LocalDate) {
        encoder.encodeString(obj.format(formatter))
    }
}

object IsoLocalDateTimeSerializer : LocalDateTimeSerializer(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

open class LocalDateTimeSerializer(private val formatter: DateTimeFormatter) : KSerializer<LocalDateTime> {
    override val descriptor: SerialDescriptor = PrimitiveDescriptor("LocalDateTime", PrimitiveKind.STRING) // StringDescriptor.withName("java.time.LocalDateTime")
    override fun deserialize(decoder: Decoder): LocalDateTime {
        return LocalDateTime.parse(decoder.decodeString(), formatter)
    }

    override fun serialize(encoder: Encoder, obj: LocalDateTime) {
        encoder.encodeString(obj.format(formatter))
    }
}

internal fun isNotOpphoert(folkeregistermetadata: Folkeregistermetadata): Boolean {
    return runCatching { LocalDateTime.now().isBefore(folkeregistermetadata.opphoerstidspunkt) }
            // TODO :: Comment in .onFailure { log.warn { "Verify isNotOpphoert faild with value ${folkeregistermetadata.opphoerstidspunkt} - ${it.localizedMessage}" } }
            .getOrDefault(true)
}

private fun Person.Bostedsadresse.findKommunenummer(): String {
    return vegadresse?.let { vegadresse ->
        Metrics.vegadresse.inc()
        vegadresse.kommunenummer }
            ?: matrikkeladresse?.let { matrikkeladresse ->
                Metrics.matrikkeladresse.inc()
                matrikkeladresse.kommunenummer }
            ?: ukjentBosted?.let { ukjentBosted ->
                Metrics.ukjentBosted.inc()
                ukjentBosted.bostedskommune }
            ?: "".also { Metrics.ingenAdresse.inc() }
}

internal fun List<Person.Bostedsadresse>.findGjelendeBostedsadresse(): Person.Bostedsadresse? {
    return this.sortedWith(nullsFirst(compareBy { it.folkeregistermetadata.gyldighetstidspunkt })).firstOrNull { isNotOpphoert(it.folkeregistermetadata) }
}

@UnstableDefault
@ImplicitReflectionSerializer
fun String.getTopicQueryFromJsonString(): TopicQueryBase = runCatching {
    json.parse(TopicQuery.serializer(), this)
    // Json.nonstrict.parse<TopicQuery>(this)
}
        .onFailure {
            log.error { "Failed serialize TopicQuery - ${it.localizedMessage}" }
        }
        .getOrDefault(InvalidTopicQuery)

@Serializable
enum class Endringstype {
    OPPRETT,
    KORRIGER,
    OPPHOER,
    ANNULER
}

@Serializable
data class Vegadresse(
    val kommunenummer: String? = null
)

@Serializable
data class Matrikkeladresse(
    val kommunenummer: String? = null
)

@Serializable
data class UkjentBosted(
    val bostedskommune: String? = null
)

@Serializable
data class Metadata(
    val master: String,
    val endringer: List<Endring>
) {
    @Serializable
    data class Endring(
        val type: Endringstype,
        @Serializable(with = IsoLocalDateTimeSerializer::class)
        val registrert: LocalDateTime,
        val registrertAv: String,
        val systemkilde: String,
        val kilde: String
    )
}
@Serializable
data class Folkeregistermetadata(
    @Serializable(with = IsoLocalDateTimeSerializer::class)
    val ajourholdstidspunkt: LocalDateTime?,
    @Serializable(with = IsoLocalDateTimeSerializer::class)
    val gyldighetstidspunkt: LocalDateTime?,
    @Serializable(with = IsoLocalDateTimeSerializer::class)
    val opphoerstidspunkt: LocalDateTime?,
    val kilde: String?,
    val aarsak: String?,
    val sekvens: Int?
)

enum class Gradering {
    STRENGT_FORTROLIG_UTLAND,
    STRENGT_FORTROLIG,
    FORTROLIG,
    UGRADERT
}

sealed class TopicQueryBase
object InvalidTopicQuery : TopicQueryBase()

@Serializable
data class TopicQuery(
    val hentPerson: Person
) : TopicQueryBase() {
    fun inRegion(r: String) = this.hentPerson.bostedsadresse.findGjelendeBostedsadresse()?.matrikkeladresse?.kommunenummer?.startsWith(r) ?: false
}

val TopicQuery.isAlive: Boolean
    get() = this.hentPerson.doedsfall.isNullOrEmpty()

@Serializable
data class Person(
    val bostedsadresse: List<Bostedsadresse>,
    val doedsfall: List<Doedsfall>
) {

    @Serializable
    data class Bostedsadresse(
        val vegadresse: Vegadresse?,
        val matrikkeladresse: Matrikkeladresse?,
        val ukjentBosted: UkjentBosted?,
        val folkeregistermetadata: Folkeregistermetadata,
        val metadata: Metadata
    )

    @Serializable
    data class Doedsfall(
        @Serializable(with = IsoLocalDateSerializer::class)
        val doedsdato: LocalDate?
    )
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

data class EventKey(
    val aktoerId: String,
    val sfObjectType: PdlSfValuesProto.SfObjectEventKey.SfObjectType
) {
    fun toProto(): PdlSfValuesProto.SfObjectEventKey =
            PdlSfValuesProto.SfObjectEventKey.newBuilder().apply {
                aktoerId = this@EventKey.aktoerId
                sfObjectType = this@EventKey.sfObjectType
            }
                    .build()
}

data class PValue(
    val idententifikasjonsnummer: String = "",
    val gradering: PdlSfValuesProto.PersonValue.Gradering = PdlSfValuesProto.PersonValue.Gradering.UGRADERT,
    val sikkerhetstiltak: String = "",
    val kommunenummer: String = "",
    val region: String = ""
) {
    fun toProto(): PdlSfValuesProto.PersonValue =
            PdlSfValuesProto.PersonValue.newBuilder().apply {
                identifikasjonsnummer = this@PValue.idententifikasjonsnummer
                gradering = this@PValue.gradering
                sikkerhetstiltak = this@PValue.sikkerhetstiltak
                kommunenummer = this@PValue.kommunenummer
                region = this@PValue.region
            }
                    .build()
}

data class AValue(
    val idententifikasjonsnummer: String = "",
    val fornavn: String = "",
    val mellomnavn: String = "",
    val etternavn: String = ""
) {
    fun toProto(): PdlSfValuesProto.AccountValue =
            PdlSfValuesProto.AccountValue.newBuilder().apply {
                identifikasjonsnummer = this@AValue.idententifikasjonsnummer
                fornavn = this@AValue.fornavn
                mellomnavn = this@AValue.mellomnavn
                etternavn = this@AValue.etternavn
            }
                    .build()
}

fun createSfObjectEventKey(aktoerId: String, sfObjectType: PdlSfValuesProto.SfObjectEventKey.SfObjectType): PdlSfValuesProto.SfObjectEventKey {
    return EventKey(
            aktoerId = aktoerId,
            sfObjectType = sfObjectType
    ).toProto()
}
