package no.nav.pdlsf

import io.kotlintest.matchers.asClue
import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.matchers.numerics.shouldBeExactly
import io.kotlintest.specs.StringSpec
import no.nav.pdlsf.proto.PdlSfValuesProto

class PdlSfValuesTests : StringSpec() {

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

        "Three identical PersonValue messages should have identical hash code " {
            PValue().toProto().hashCode().let { hashCode ->
                (1..3).map { PValue().toProto().hashCode() }.toSet().asClue {
                    it.shouldHaveSize(1)
                    it.first() shouldBeExactly hashCode
                }
            }
        }

        "Three different PersonValue messages should have different hash code" {
            listOf(
                    PValue(idententifikasjonsnummer = "Torstein"),
                    PValue(idententifikasjonsnummer = "TorsteiN"),
                    PValue(idententifikasjonsnummer = "TorsTein")
            ).map { it.toProto().hashCode() }.toSet() shouldHaveSize(3)
        }

        "Three different gradering should give different hash code" {
            listOf(
                    PValue(gradering = PdlSfValuesProto.PersonValue.Gradering.UGRADERT),
                    PValue(gradering = PdlSfValuesProto.PersonValue.Gradering.FORTROLIG),
                    PValue(gradering = PdlSfValuesProto.PersonValue.Gradering.STRENGT_FORTROLIG)
            ).map { it.toProto().hashCode() }.toSet() shouldHaveSize(3)
        }
    }
}
