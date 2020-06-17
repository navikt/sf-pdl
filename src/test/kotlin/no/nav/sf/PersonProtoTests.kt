package no.nav.sf

import io.kotest.assertions.asClue
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldBe
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.UnstableDefault
import no.nav.pdlsf.proto.PersonProto
import no.nav.sf.pdl.AdressebeskyttelseGradering
import no.nav.sf.pdl.PersonSf
import no.nav.sf.pdl.Query
import no.nav.sf.pdl.UKJENT_FRA_PDL
import no.nav.sf.pdl.getQueryFromJson
import no.nav.sf.pdl.toPersonSf

private const val AKTORID = "AKTORID"
private const val FOLKEREGISTERIDENT = "FOLKEREGISTERIDENT"

private const val PDL_TOPIC_VALUE_OK_WITHOUT_HISTORIKK = "/pdlTopicValues/value.json"

@OptIn(UnstableDefault::class)
@ImplicitReflectionSerializer
class PersonProtoTests : StringSpec() {

    init {

        "Three identical PersonValue messages should have identical hash code " {
            PersonSf().toPersonProto().second.hashCode().let { hashCode ->
                (1..3).map { PersonSf().toPersonProto().second.hashCode() }.toSet().asClue {
                    it.shouldHaveSize(1)
                    it.first() shouldBeExactly hashCode
                }
            }
        }

        "Three different PersonValue messages should have different hash code" {
            listOf(
                    PersonSf(identifikasjonsnummer = "Torstein"),
                    PersonSf(identifikasjonsnummer = "TorsteiN"),
                    PersonSf(identifikasjonsnummer = "TorsTein")
            ).map { it.toPersonProto().second.hashCode() }.toSet() shouldHaveSize(3)
        }

        "Three different gradering should give different hash code" {
            listOf(
                    PersonSf(adressebeskyttelse = AdressebeskyttelseGradering.UGRADERT),
                    PersonSf(adressebeskyttelse = AdressebeskyttelseGradering.FORTROLIG),
                    PersonSf(adressebeskyttelse = AdressebeskyttelseGradering.STRENGT_FORTROLIG)
            ).map { it.toPersonProto().second.hashCode() }.toSet() shouldHaveSize(3)
        }
    }

    init {
        "Pdl topic value to Proto - value.json" {
            val queryBase = getStringFromResource(PDL_TOPIC_VALUE_OK_WITHOUT_HISTORIKK).getQueryFromJson() as Query
            val personSf = queryBase.toPersonSf() as PersonSf
            val proto = personSf.toPersonProto()

            proto.first.aktoerId shouldBe AKTORID

            proto.second.identifikasjonsnummer shouldBe FOLKEREGISTERIDENT
            proto.second.fornavn shouldBe UKJENT_FRA_PDL
            proto.second.mellomnavn shouldBe UKJENT_FRA_PDL
            proto.second.etternavn shouldBe UKJENT_FRA_PDL
            proto.second.adressebeskyttelse shouldBe PersonProto.PersonValue.Gradering.UGRADERT
            proto.second.sikkerhetstiltakList shouldBe emptyList<String>()
            proto.second.kommunenummer shouldBe UKJENT_FRA_PDL
            proto.second.region shouldBe UKJENT_FRA_PDL
            proto.second.doed shouldBe false
        }
    }
}

@ImplicitReflectionSerializer
internal fun getStringFromResource(path: String) =
        PersonProtoTests::class.java.getResourceAsStream(path).bufferedReader().use { it.readText() }
