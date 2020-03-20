package no.nav.pdlsf

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.ImplicitReflectionSerializer
import mu.KotlinLogging

@ImplicitReflectionSerializer
object Bootstrap {

    private val log = KotlinLogging.logger { }

    fun start(params: Params = ParamsFactory.p) {

        ShutdownHook.reset()

        NaisDSL.enabled { conditionalSchedule(params) } // end of use for NAISDSL - shutting down REST API
    }

    private tailrec fun conditionalSchedule(params: Params) {

        // some resets before next attempt/work session
        ServerState.reset()
        Metrics.sessionReset()
        work(params) // ServerState will be updated according to any issues
        // wait(params)

        if (!ShutdownHook.isActive()) conditionalSchedule(params)
    }

    private fun wait(params: Params) {
        val msDelay = params.msBetweenWork
        log.info { "Will wait $msDelay ms before starting all over" }
        runCatching { runBlocking { delay(msDelay) } }
            .onSuccess { log.info { "waiting completed" } }
            .onFailure { log.info { "waiting interrupted" } }
    }
}
