package no.nav.pdlsf

import java.net.URI
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import org.apache.http.HttpHost
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.HttpClients
import org.http4k.client.ApacheClient
import org.http4k.core.HttpHandler
import org.http4k.core.Request
import org.http4k.core.Response

// internal val json = Json(JsonConfiguration.Stable)
internal val json = Json(JsonConfiguration(
        ignoreUnknownKeys = true)
)

object Http {
    val client: HttpHandler by lazy { ApacheClient.proxy() }
}

fun ApacheClient.proxy(): HttpHandler = when {
    ParamsFactory.p.httpsProxy.isEmpty() -> this()
    else -> {
        val up = URI(ParamsFactory.p.httpsProxy)
        this(client =
        HttpClients.custom()
                .setDefaultRequestConfig(
                        RequestConfig.custom()
                                .setProxy(HttpHost(up.host, up.port, up.scheme))
                                .setRedirectsEnabled(false)
                                .setCookieSpec(CookieSpecs.IGNORE_COOKIES)
                                .build())
                .build()
        )
    }
}

fun HttpHandler.invokeWM(r: Request): Response = Metrics.responseLatency.startTimer().let { rt ->
    this.invoke(r).also { rt.observeDuration() }
}
