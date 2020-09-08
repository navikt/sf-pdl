package no.nav.sf.pdl

import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.PrestopHook
import no.nav.sf.library.ShutdownHook
import no.nav.sf.library.enableNAISAPI

private const val EV_bootstrapWaitTime = "MS_BETWEEN_WORK" // default to 10 minutes
private val bootstrapWaitTime = AnEnvironment.getEnvOrDefault(EV_bootstrapWaitTime, "60000").toLong()

/**
 * Bootstrap is a very simple µService manager
 * - start, enables mandatory NAIS API before entering 'work' loop
 * - loop,  invokes a work -, then a wait session, until shutdown - or prestop hook (NAIS feature),
 * - conditionalWait, waits a certain time, checking the hooks once a while
 */

object Bootstrap {

    private val log = KotlinLogging.logger { }

    fun start(ws: WorkSettings = WorkSettings()) {
        log.info { "Starting" }
        enableNAISAPI {
            if (ws.initialLoad || FilterBase.filterSettingsDiffer(ws.filterEnabled, ws.filter, ws.prevEnabled, ws.prevFilter)) {
                if (ws.initialLoad) {
                    log.info { "Initial load flag set will trigger initial load - will build populationCache from beginning of pdl topic and post latest to sf-person" }
                } else {
                    log.info { "Filter changed since last run will trigger initial load - will build populationCache from beginning of pdl topic and post latest to sf-person" }
                }

                val startupOffset = getStartupOffset<String, Any>(ws.kafkaConsumerPdl)
                if (startupOffset == 0L) {
                    log.error { "Failed finding startupOffset" }
                    return@enableNAISAPI
                }
                if (!initLoad(ws).isOK()) {
                    log.error { "Failed loading population" }
                    return@enableNAISAPI
                }
                log.info { "Initial load done." }
                conditionalWait()
                loop(ws.copy(
                        startUpOffset = startupOffset
                )
                )
            } else {
                loop(ws)
            }
        }
        log.info { "Finished!" }
    }

    private tailrec fun loop(ws: WorkSettings) {
        log.info { "Ready to loop" }
        val stop = ShutdownHook.isActive() || PrestopHook.isActive()
        when {
            stop -> Unit
            !stop -> {
                log.info { "Continue to loop" }
                loop(work(ws)
                    .let { prevWS ->
                        prevWS.first.copy(
                                prevFilter = FilterBase.fromS3(), // Fetch filter from last successful work session
                                prevEnabled = FilterBase.flagFromS3(),
                                cache = prevWS.third,
                                startUpOffset = -1L // Only seek to startUpOffset on first work session
                        )
                    }
                    .also { conditionalWait() })
            }
        }
    }

    private fun conditionalWait(ms: Long = bootstrapWaitTime) =
            runBlocking {
                log.info { "Will wait $ms ms before starting all over" }

                val cr = launch {
                    runCatching { delay(ms) }
                            .onSuccess { log.info { "waiting completed" } }
                            .onFailure { log.info { "waiting interrupted" } }
                }

                tailrec suspend fun loop(): Unit = when {
                    cr.isCompleted -> Unit
                    ShutdownHook.isActive() || PrestopHook.isActive() -> cr.cancel()
                    else -> {
                        delay(250L)
                        loop()
                    }
                }

                loop()
                cr.join()
            }
}
