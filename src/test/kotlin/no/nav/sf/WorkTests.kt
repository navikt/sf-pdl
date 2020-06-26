package no.nav.sf
import io.findify.s3mock.S3Mock
import io.kotest.assertions.asClue
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import no.nav.sf.pdl.Cache
import no.nav.sf.pdl.ExitReason
import no.nav.sf.pdl.FilterBase
import no.nav.sf.pdl.PersonInvalid
import no.nav.sf.pdl.PersonSf
import no.nav.sf.pdl.PersonTombestone
import no.nav.sf.pdl.Query
import no.nav.sf.pdl.WorkSettings
import no.nav.sf.pdl.toPersonSf
import no.nav.sf.pdl.work

private const val QUERY_JSON1 = "/queryJson/query1.json"
private const val QUERY_JSON2 = "/queryJson/query2.json"
private const val QUERY_JSON3 = "/queryJson/query3.json"

private val jsonNonStrict = Json(JsonConfiguration.Stable.copy(ignoreUnknownKeys = true, isLenient = true))

@ExperimentalStdlibApi
@ImplicitReflectionSerializer
class WorkTests : StringSpec() {

    init {
        val api = S3Mock.Builder().withPort(8001).withInMemoryBackend().build()
        api.start()

        "Verify mapping of query to PersonSf" {
            val query1 = jsonNonStrict.parse(Query.serializer(), getStringFromResource(QUERY_JSON1))
            val query2 = jsonNonStrict.parse(Query.serializer(), getStringFromResource(QUERY_JSON2))
            val query3 = jsonNonStrict.parse(Query.serializer(), getStringFromResource(QUERY_JSON3))

            query1.toPersonSf() shouldNotBe PersonInvalid
            query2.toPersonSf() shouldNotBe PersonInvalid
            query3.toPersonSf() shouldNotBe PersonInvalid
        }

        "Verify exists check on cache" {

            val aktoerIdOne = "a1"
            val personOne = PersonSf(
                    aktoerId = aktoerIdOne,
                    identifikasjonsnummer = "11"
            )
            val aktoerIdTwo = "a2"
            val personTwo = PersonSf(
                    aktoerId = aktoerIdTwo,
                    identifikasjonsnummer = "22"
            )
            val aktoerIdThree = "a3"
            val personThree = PersonSf(
                    aktoerId = aktoerIdThree,
                    identifikasjonsnummer = "33"
            )

            val aktoerIdNew = "a4"
            val personNew = PersonSf(
                    aktoerId = aktoerIdNew,
                    identifikasjonsnummer = "44"
            )
            val personUpdatedAktoerIdThree = PersonSf(
                    aktoerId = aktoerIdThree,
                    identifikasjonsnummer = "333"
            )

            val aktoerIdFive = "a5"
            val personTombestone = PersonTombestone(aktoerId = aktoerIdFive)

            val personCache = Cache.Exist(
                    map = mapOf(
                            aktoerIdOne to personOne.toPersonProto().second.hashCode(),
                            aktoerIdTwo to personTwo.toPersonProto().second.hashCode(),
                            aktoerIdThree to personThree.toPersonProto().second.hashCode(),
                            aktoerIdFive to null
                    )
            )
            personCache.isNewOrUpdated(personNew.toPersonProto()) shouldBe true
            personCache.isNewOrUpdated(personUpdatedAktoerIdThree.toPersonProto()) shouldBe true
            personCache.isNewOrUpdated(personOne.toPersonProto()) shouldBe false
            personCache.isNewOrUpdated(Pair(personTombestone.toPersonTombstoneProtoKey(), null)) shouldBe false
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

    @ImplicitReflectionSerializer
    internal fun getStringFromResource(path: String) =
            WorkTests::class.java.getResourceAsStream(path).bufferedReader().use { it.readText() }
}
