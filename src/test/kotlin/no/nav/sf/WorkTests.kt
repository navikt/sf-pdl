package no.nav.sf
import io.kotest.assertions.asClue
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.ImplicitReflectionSerializer
import no.nav.sf.pdl.ExitReason
import no.nav.sf.pdl.FilterBase
import no.nav.sf.pdl.ObjectInCacheStatus
import no.nav.sf.pdl.PersonSf
import no.nav.sf.pdl.WorkSettings
import no.nav.sf.pdl.exists
import no.nav.sf.pdl.work

@ExperimentalStdlibApi
@ImplicitReflectionSerializer
class WorkTests : StringSpec() {

    init {
        "Verify exists check on cache" {

            val aktoerIdOne = "a1"
            val personOne = PersonSf(identifikasjonsnummer = "1")
            val aktoerIdTwo = "a2"
            val personTwo = PersonSf(identifikasjonsnummer = "2")
            val aktoerIdThree = "a3"
            val personThree = PersonSf(identifikasjonsnummer = "3")

            val aktoerIdNew = "a4"
            val personNew = PersonSf(identifikasjonsnummer = "4")
            val personUpdated = PersonSf(identifikasjonsnummer = "5")

            val personCache = mapOf(
                    aktoerIdOne to personOne.hashCode(),
                    aktoerIdTwo to personTwo.hashCode(),
                    aktoerIdThree to personThree.hashCode()
            )

            personCache.exists(aktoerIdOne, personOne.hashCode()) shouldBe ObjectInCacheStatus.NoChange
            personCache.exists(aktoerIdNew, personNew.hashCode()) shouldBe ObjectInCacheStatus.New
            personCache.exists(aktoerIdOne, personUpdated.hashCode()) shouldBe ObjectInCacheStatus.Updated
        }

        "FilterPersonBase fromJson should work as expected" {

            val invalidJson1 = """invalid json"""

            val validJson = """
                {
                    "regions": [
                        {
                            "region" : "54" ,
                            "municipals" : []
                        } ,
                        {
                            "region": "18",
                            "municipals": ["1804" , "1806"]
                        } ]
                }
            """.trimIndent()

            FilterBase.fromJson(invalidJson1)
                    .shouldBeInstanceOf<FilterBase.Missing>()

            FilterBase.fromJson(validJson)
                    .shouldBeInstanceOf<FilterBase.Exists>().asClue {
                        it.regions.isEmpty() shouldBe false
                        it.regions[0].region shouldBe "54"
                        it.regions[0].municipals shouldHaveSize 0
                        it.regions[1].region shouldBe "18"
                        it.regions[1].municipals shouldHaveSize 2
                    }
        }

        "work should exit correctly for different situations - NoFilter" {

            work(WorkSettings(filter = FilterBase.Missing)).second.shouldBeInstanceOf<ExitReason.NoFilter>()
        }
    }
}
