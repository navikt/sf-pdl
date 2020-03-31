package no.nav.pdlsf

import java.time.LocalDate
import java.time.LocalDateTime
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.UnstableDefault
import kotlinx.serialization.stringify
import mu.KotlinLogging
import no.nav.pdlsf.proto.PdlSfValuesProto
import org.http4k.core.Method
import org.http4k.core.Status

private val log = KotlinLogging.logger { }
private const val GRAPHQL_QUERY = "/graphql/query.graphql"

@ImplicitReflectionSerializer
private fun executeGraphQlQuery(
    query: String,
    variables: Map<String, String>
): QueryResponseBase = Http.client.invoke(
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
    return if (!stringResponse.isNullOrEmpty()) {
        runCatching {
            val queryResponse = stringResponse.getQueryResponseFromJsonString()
            val result = if (queryResponse is QueryResponse) {
                val queryResponseBase = if (queryResponse.errors.isNullOrEmpty()) queryResponse else QueryErrorResponse(queryResponse.errors)
                queryResponseBase
            } else {
                InvalidQueryResponse
            }
            log.debug { "GraphQL result $result" }
            result
        }
        .onFailure {
            log.debug { "GaphQL response string - $stringResponse" } // TODO :: REMOVE
            log.error { "Failed handling graphql response - ${it.localizedMessage}" }
        }
        .getOrDefault(InvalidQueryResponse)
    } else {
        InvalidQueryResponse
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

fun QueryResponse.toPersonValue(): PdlSfValuesProto.PersonValue {
    return PValue(
            idententifikasjonsnummer = this.data.hentIdenter.identer.first().ident,
            gradering = runCatching { this.data.hentPerson.adressebeskyttelse.first().gradering.name }.getOrDefault(Gradering.UGRADERT.name).let { PdlSfValuesProto.PersonValue.Gradering.valueOf(it) },
            sikkerhetstiltak = this.data.hentPerson.sikkerhetstiltak.first().beskrivelse,
            kommunenummer = this.data.hentPerson.bostedsadresse.first().findKommunenummer(),
            region = this.data.hentPerson.bostedsadresse.first().findKommunenummer().substring(0, 2)
    ).toProto()
}

fun QueryResponse.toAccountValue(): PdlSfValuesProto.AccountValue {
    return AValue(
            idententifikasjonsnummer = this.data.hentIdenter.identer.first().ident,
            fornavn = this.data.hentPerson.navn.first().fornavn,
            mellomnavn = this.data.hentPerson.navn.first().mellomnavn.orEmpty(),
            etternavn = this.data.hentPerson.navn.first().etternavn
    ).toProto()
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
    json.parse(QueryResponse.serializer(), this)
    // Json.nonstrict.parse<TopicQuery>(this)
}
        .onFailure {
            log.error { "Failed serialize GraphQL QueryResponse - ${it.localizedMessage}" }
        }
        .getOrDefault(InvalidQueryResponse)
