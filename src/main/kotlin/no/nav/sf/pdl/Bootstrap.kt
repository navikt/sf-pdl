package no.nav.sf.pdl

import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.ImplicitReflectionSerializer
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

@ImplicitReflectionSerializer
@ExperimentalStdlibApi
object Bootstrap {

    private val log = KotlinLogging.logger { }

    fun start(ws: WorkSettings = WorkSettings()) {
        log.info { "Starting" }
        enableNAISAPI { loop(ws) }
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
                                prevEnabled = FilterBase.flagFromS3()
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
