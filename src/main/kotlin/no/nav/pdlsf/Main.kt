package no.nav.pdlsf

import kotlinx.serialization.ImplicitReflectionSerializer
import mu.KotlinLogging

@ImplicitReflectionSerializer
fun main() {
    val log = KotlinLogging.logger {}

    log.info { "Starting" }

    log.info { "Checking environment variables" }
    ParamsFactory.p.let { params ->

        log.info { "Proxy details: ${params.httpsProxy}" }

        if (params.kafkaSecurityEnabled() && !params.kafkaSecurityComplete()) {
            log.error { "Kafka security enabled, but incomplete kafka security properties - " }
            return
        }
    }

    Bootstrap.start()

    log.info { "Finished!" }
}
