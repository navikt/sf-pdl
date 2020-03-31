package no.nav.pdlsf
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import no.nav.pdlsf.proto.PdlSfValuesProto

class WorkTests : StringSpec() {

    private data class PValue(
        val idententifikasjonsnummer: String = "",
        val gradering: PdlSfValuesProto.PersonValue.Gradering = PdlSfValuesProto.PersonValue.Gradering.UGRADERT,
        val sikkerhetstiltak: String = "",
        val kommunenummer: String = "",
        val region: String = ""
    ) {
        fun toProto(): PdlSfValuesProto.PersonValue =
                PdlSfValuesProto.PersonValue.newBuilder().apply {
                    identifikasjonsnummer = this@PValue.idententifikasjonsnummer
                    gradering = this@PValue.gradering
                    sikkerhetstiltak = this@PValue.sikkerhetstiltak
                    kommunenummer = this@PValue.kommunenummer
                    region = this@PValue.region
                }
                        .build()
    }

    init {
        val aktoerIdOne = "a1"
        val personOne = PValue(idententifikasjonsnummer = "1")
        val aktoerIdTwo = "a2"
        val personTwo = PValue(idententifikasjonsnummer = "2")
        val aktoerIdThree = "a3"
        val personThree = PValue(idententifikasjonsnummer = "3")

        val aktoerIdNew = "a4"
        val personNew = PValue(idententifikasjonsnummer = "4")
        val personUpdated = PValue(idententifikasjonsnummer = "5")

        val personCache = mapOf(
                aktoerIdOne to personOne.hashCode(),
                aktoerIdTwo to personTwo.hashCode(),
                aktoerIdThree to personThree.hashCode()
        )

        "Verify exists check on cache" {
            personCache.exists(aktoerIdOne, personOne.hashCode()) shouldBe ObjectInCacheStatus.NoChange
            personCache.exists(aktoerIdNew, personNew.hashCode()) shouldBe ObjectInCacheStatus.New
            personCache.exists(aktoerIdOne, personUpdated.hashCode()) shouldBe ObjectInCacheStatus.Updated
        }
    }
}
