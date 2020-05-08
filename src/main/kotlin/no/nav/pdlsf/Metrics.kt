package no.nav.pdlsf

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import io.prometheus.client.hotspot.DefaultExports
import mu.KotlinLogging

enum class AdresseType {
    INGEN,
    MATRIKKELADRESSE,
    VEGADRESSE,
    UKJENTBOSTED
}

object Metrics {

    private val log = KotlinLogging.logger { }

    val cRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry

    val preStopHook: Gauge = Gauge
            .build()
            .name("pre_stop__hook_gauge")
            .help("No. of preStopHook activation since ever")
            .register()

    val failedRequest: Gauge = Gauge
            .build()
            .name("failed_request_gauge")
            .help("No. of failed requests to Salesforce since last restart")
            .register()

    val responseLatency: Histogram = Histogram
        .build()
        .name("response_latency_seconds_histogram")
        .labelNames("uri")
        .help("Http response latency")
        .register()

    val noOfKakfaRecordsPdl: Gauge = Gauge
            .build()
            .name("no_kafkarecords_pdl_gauge")
            .help("No. of kafka records pdl")
            .register()

    val invalidQuery: Gauge = Gauge
            .build()
            .name("invalid_query_gauge")
            .help("No. of failed kafka values converted to query on topic since last restart")
            .register()

    val sucessfulValueToQuery: Gauge = Gauge
            .build()
            .name("sucessfully_value_to_query_gauge")
            .help("No of sucessfully converted kafka topic values to query")
            .register()

    val cachedPersons: Gauge = Gauge
            .build()
            .name("cached_persons_event_gauge")
            .help("No. of cached persons consumed in last work session")
            .register()

    val publishedPersons: Gauge = Gauge
            .build()
            .name("published_person_gauge")
            .labelNames("status")
            .help("No. of persons published to kafka in last work session")
            .register()

    val parsedGrapQLPersons: Gauge = Gauge
            .build()
            .name("parsed_person_gauge")
            .labelNames("type")
            .help("No. of person types parsed from graphql response in last work session")
            .register()

    val usedAdresseTypes: Gauge = Gauge
            .build()
            .name("used_adress_gauge")
            .labelNames("type")
            .help("No. of adress types used from graphql response in last work session")
            .register()

    init {
        DefaultExports.initialize()
        log.info { "Prometheus metrics are ready" }
    }

    fun sessionReset() {
        cachedPersons.clear()
        publishedPersons.clear()
        parsedGrapQLPersons.clear()

        usedAdresseTypes.clear()

        invalidQuery.clear()
        sucessfulValueToQuery.clear()

        responseLatency.clear()
        noOfKakfaRecordsPdl.clear()
    }
}
