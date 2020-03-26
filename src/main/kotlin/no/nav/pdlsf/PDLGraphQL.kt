package no.nav.pdlsf

import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.stringify
import mu.KotlinLogging
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
).also { log.debug { "Respone ${it.toMessage() }" }
}.let { response ->
    when (response.status) {
        Status.OK -> {
            log.debug { "GraphQL response ${response.bodyString()}" }
            runCatching {
                val queryResponse = QueryResponse.fromJson(response.bodyString())
                val result = if (queryResponse is QueryResponse) {
                    queryResponse.errors?.let { errors -> QueryErrorResponse(errors) } ?: queryResponse
                } else {
                    queryResponse
                }
                result
            }
                    .onFailure { "Failed handlin graphql response - ${it.localizedMessage}" }
                    .getOrDefault(InvalidQueryResponse)
        }
        else -> {
            log.error { "Request failed - ${response.toMessage()}" }
            InvalidQueryResponse
        }
    }
}

@ImplicitReflectionSerializer
fun queryGraphQlSFDetails(ident: String): QueryResponseBase {
        val query = getStringFromResource(GRAPHQL_QUERY).trim()
        return executeGraphQlQuery(query, mapOf("ident" to ident))
}
