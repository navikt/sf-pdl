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
import kotlinx.serialization.stringify
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import org.http4k.core.Method
import org.http4k.core.Status

private val log = KotlinLogging.logger { }
private const val GRAPHQL_QUERY = "/graphql/query.graphql"

@OptIn(UnstableDefault::class)
@UnstableDefault
@ImplicitReflectionSerializer
private fun executeGraphQlQuery(
    query: String,
    variables: Map<String, String>
) = Http.client.invoke(
        org.http4k.core.Request(Method.POST, ParamsFactory.p.pdlGraphQlUrl)
                .header("x-nav-apiKey", ParamsFactory.p.pdlGraphQlApiKey)
                .header("Tema", "GEN")
                .header("Authorization", "Bearer ${(getStsToken() as StsAccessToken).accessToken}")
                .header("Nav-Consumer-Token", "Bearer ${(getStsToken() as StsAccessToken).accessToken}")
                .header("Cache-Control", "no-cache")
                .header("Content-Type", "application/json")
                .body(json.stringify(QueryRequest(
                        query = query,
                        variables = variables
                )))
).let { response ->
    when (response.status) {
        Status.OK -> {
            log.debug { "GraphQL response ${response.bodyString()}" }
            runCatching {
                val queryResponse = response.bodyString().getQueryResponseFromJsonString()
                val result = if (queryResponse is QueryResponse) {
                    queryResponse.errors?.let { errors -> QueryErrorResponse(errors) } ?: queryResponse
                } else {
                    queryResponse
                }
                log.debug { "GraphQL result $result" }
                result
            }
                    .onFailure { "Failed handling graphql response - ${it.localizedMessage}" }
                    .getOrDefault(InvalidQueryResponse)
        }
        else -> {
            log.error { "PDL GraphQl request failed - ${response.toMessage()}" }
            InvalidQueryResponse
        }
    }
}

@ImplicitReflectionSerializer
private fun executeGraphQlQueryStringResponse(
    query: String,
    variables: Map<String, String>
): String = Http.client.invoke(
        org.http4k.core.Request(Method.POST, ParamsFactory.p.pdlGraphQlUrl)
                .header("x-nav-apiKey", ParamsFactory.p.pdlGraphQlApiKey)
                .header("Tema", "GEN")
                .header("Authorization", "Bearer ${(getStsToken() as StsAccessToken).accessToken}")
                .header("Nav-Consumer-Token", "Bearer ${(getStsToken() as StsAccessToken).accessToken}")
                .header("Cache-Control", "no-cache")
                .header("Content-Type", "application/json")
                .body(json.stringify(QueryRequest(
                        query = query,
                        variables = variables
                )))
).let { response ->
    when (response.status) {
        Status.OK -> {
            log.debug { "GraphQL response ${response.bodyString()}" } // TODO :: REMOVE
            response.bodyString()
        }
        else -> {
            log.error { "PDL GraphQl request failed - ${response.toMessage()}" }
            ""
        }
    }
}

@ImplicitReflectionSerializer
fun queryGraphQlSFDetails(ident: String): QueryResponseBase {
    val query = getStringFromResource(GRAPHQL_QUERY).trim()
    val stringResponse = executeGraphQlQueryStringResponse(query, mapOf("ident" to ident))
    log.debug { "GaphQL response string - $stringResponse" } // TODO :: REMOVE
    return if (stringResponse.isNotEmpty()) {
        parseGraphQLResponse(stringResponse)
    } else {
        InvalidQueryResponse
    }
}

@OptIn(UnstableDefault::class)
@ImplicitReflectionSerializer
fun parseGraphQLResponse(stringResponse: String): QueryResponseBase {
    return runCatching {
        val result = stringResponse.getQueryResponseFromJsonString()
        log.debug { "GraphQL result $result" }
        result
    }
            .onFailure {
                log.debug { "GaphQL response string on failure- $stringResponse" } // TODO :: REMOVE
                log.error { "Failed handling graphql response - ${it.localizedMessage}" }
            }
            .getOrDefault(InvalidQueryResponse)
}

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
            val doedsfall: List<Doedsfall>,
            val navn: List<Navn>,
            val sikkerhetstiltak: List<Sikkerhetstiltak>
        ) {
            @Serializable
            data class Adressebeskyttelse(
                val gradering: Gradering
            )

            @Serializable
            data class Bostedsadresse(
                val vegadresse: Vegadresse?,
                val matrikkeladresse: Matrikkeladresse?,
                val ukjentBosted: UkjentBosted?
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
            data class Doedsfall(
                @Serializable(with = IsoLocalDateSerializer::class)
                val doedsdato: LocalDate
            )
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
                val beskrivelse: String
            )
        }
        @Serializable
        data class HentIdenter(
            val identer: List<Identer>
        ) {
            @Serializable
            data class Identer(
                val ident: String,
                val gruppe: IdentGruppe
            ) {
                @Serializable
                enum class IdentGruppe {
                    AKTORID, FOLKEREGISTERIDENT, NPID
                }
            }
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
}

@Serializable
data class QueryErrorResponse(
    val errors: List<QueryResponse.Error>?
) : QueryResponseBase()

@Serializable
data class QueryRequest(
    val query: String,
    val variables: Map<String, String>,
    val operationName: String? = null
) {
    data class Variables(
        val variables: Map<String, Any>
    )
}

enum class Gradering {
    STRENGT_FORTROLIG_UTLAND,
    STRENGT_FORTROLIG,
    FORTROLIG,
    UGRADERT
}

fun QueryResponse.toPerson(): Person {
    return runCatching { Person(
            aktoerId = this.data.hentIdenter.identer.first { it.gruppe == QueryResponse.Data.HentIdenter.Identer.IdentGruppe.AKTORID }.ident,
            identifikasjonsnummer = this.data.hentIdenter.identer.first { it.gruppe == QueryResponse.Data.HentIdenter.Identer.IdentGruppe.FOLKEREGISTERIDENT }.ident,
            fornavn = this.data.hentPerson.navn.filter { it.metadata.master.equals("FREG") }.first().fornavn,
            mellomnavn = this.data.hentPerson.navn.filter { it.metadata.master.equals("FREG") }.first().mellomnavn.orEmpty(),
            etternavn = this.data.hentPerson.navn.filter { it.metadata.master.equals("FREG") }.first().etternavn,
            adressebeskyttelse = runCatching { this.data.hentPerson.adressebeskyttelse.first().gradering.name }.getOrDefault(Gradering.UGRADERT.name).let { PersonProto.PersonValue.Gradering.valueOf(it) },
            sikkerhetstiltak = this.data.hentPerson.sikkerhetstiltak.map { it.beskrivelse }.toList(),
            kommunenummer = runCatching { this.data.hentPerson.bostedsadresse.first().findKommunenummer() }.getOrDefault(""),
            region = runCatching { this.data.hentPerson.bostedsadresse.first().findKommunenummer().substring(0, 2) }.getOrDefault(""),
            doed = this.data.hentPerson.doedsfall.isNotEmpty()
        )
    }
            .onFailure { log.error { "Error creating Person from a graphQL query response ${it.localizedMessage}" } }
            .getOrThrow()
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

@UnstableDefault
@ImplicitReflectionSerializer
fun String.getQueryResponseFromJsonString(): QueryResponseBase = runCatching {
    runCatching {
        json.parse(QueryResponse.serializer(), this)
    }.getOrNull()?.let { it } ?: json.parse(QueryErrorResponse.serializer(), this)
}
        .onFailure {
            log.error { "Failed serialize GraphQL QueryResponse - ${it.localizedMessage}" }
        }
        .getOrDefault(InvalidQueryResponse)
