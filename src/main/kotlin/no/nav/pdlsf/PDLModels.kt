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
import org.http4k.core.Status

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

fun QueryResponse.Data.HentPerson.Bostedsadresse.findKommunenummer(): String {
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

@UnstableDefault
@ImplicitReflectionSerializer
fun String.getQueryResponseFromJsonString(): QueryResponseBase = runCatching {
    json.parse(QueryResponse.serializer(), this)
    // Json.nonstrict.parse<TopicQuery>(this)
}
        .onFailure {
            log.error { "Failed serialize TopicQuery - ${it.localizedMessage}" }
        }
        .getOrDefault(InvalidQueryResponse)

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

sealed class QueryResponseBase
object InvalidQueryResponse : QueryResponseBase()

@Serializable
data class QueryResponse(
    val data: Data,
    val errors: List<Error>? = emptyList()
) : QueryResponseBase() {
    @Serializable
    data class Data(
        val hentPerson: HentPerson,
        val hentIdenter: HentIdenter
    ) {
        @Serializable
        data class HentPerson(
            val adressebeskyttelse: List<Adressebeskyttelse>,
            val bostedsadresse: List<Bostedsadresse>,
            val navn: List<Navn>,
            val sikkerhetstiltak: List<Sikkerhetstiltak>
        ) {
            @Serializable
            data class Adressebeskyttelse(
                val gradering: Gradering,
                val folkeregistermetadata: Folkeregistermetadata?
            ) {
                @Serializable
                data class Folkeregistermetadata(
                    @Serializable(with = IsoLocalDateTimeSerializer::class)
                    val opphoerstidspunkt: LocalDateTime?
                )
            }
            @Serializable
            data class Bostedsadresse(
                val vegadresse: Vegadresse? = null, // TODO :: fjerne ?
                val matrikkeladresse: Matrikkeladresse? = null,
                val ukjentBosted: UkjentBosted? = null
            ) {
                @Serializable
                data class Vegadresse(
                    val kommunenummer: String
                )
                @Serializable
                data class Matrikkeladresse(
                    val kommunenummer: String
                )
                @Serializable
                data class UkjentBosted(
                    val bostedskommune: String
                )
            }
            @Serializable
            data class Navn(
                val fornavn: String,
                val mellomnavn: String?,
                val etternavn: String,
                val metadata: Metadata
            ) {
                @Serializable
                data class Metadata(
                    val master: String
                )
            }
            @Serializable
            data class Sikkerhetstiltak(
                val beskrivelse: String,
                @Serializable(with = IsoLocalDateSerializer::class)
                val gyldigTilOgMed: LocalDate
            )
        }
        @Serializable
        data class HentIdenter(
            val identer: List<Identer>
        ) {
            @Serializable
            data class Identer(
                val ident: String
            )
        }
    }

    @Serializable
    data class Error(
        val extensions: Extensions,
        val locations: List<Location>,
        val path: List<String>,
        val message: String
    ) {
        @Serializable
        data class Extensions(
            val classification: String,
            val code: String?
        )
        @Serializable
        data class Location(
            val column: Int,
            val line: Int
        )

        fun mapToHttpCode(): Status = when (this.extensions.code) {
            "unauthenticated" -> Status.FORBIDDEN
            "unauthorized" -> Status.UNAUTHORIZED
            "not_found" -> Status.NOT_FOUND
            "bad_request" -> Status.BAD_REQUEST
            "server_error" -> Status.INTERNAL_SERVER_ERROR
            else -> Status.INTERNAL_SERVER_ERROR
        }
    }
//    companion object {
//        fun fromJson(data: String): QueryResponseBase = runCatching { json.parse(serializer(), data) }
//                .onFailure { "Parsing of query response failed - ${it.localizedMessage}" }
//                .getOrDefault(InvalidQueryResponse)
//    }
}

@Serializable
data class QueryErrorResponse(
    val errors: List<QueryResponse.Error>?
) : QueryResponseBase() {

    fun fromJson(data: String): QueryResponseBase = runCatching { json.parse(serializer(), data) }
            .onFailure { "Parsing of query response failed - ${it.localizedMessage}" }
            .getOrDefault(InvalidQueryResponse)
}

@Serializable
data class QueryRequest(
    val query: String,
    val variables: Map<String, String>, // @ContextualSerialization val variables: Variables          Spennende å se om den fungerer, fungerer det ikke kan vi bare sette Map<String, String>
    val operationName: String? = null
) {
    data class Variables(
        val variables: Map<String, Any>
    )
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
