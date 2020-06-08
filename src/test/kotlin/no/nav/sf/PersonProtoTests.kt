package no.nav.sf
/*

import io.kotlintest.matchers.asClue
import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.matchers.haveSize
import io.kotlintest.matchers.numerics.shouldBeExactly
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.UnstableDefault
import no.nav.pdlsf.proto.PersonProto

private const val AKTORID = "AKTORID"
private const val FOLKEREGISTERIDENT = "FOLKEREGISTERIDENT"

private const val ALLE_ELEMENTER_UNDER_HENTPERSON_TOMME = "/graphQLResponses/AllListsUnderHentPersonIsEmpty.json"
private const val KUN_AKTOERID_UNDER_HENTIDENTER_ALLE_ELEMENTER_UNDER_HENTPERSON_TOMME = "/graphQLResponses/OnlyHentIdenterAktoerId.json"
private const val KUN_AKTOERID_UNDER_HENTIDENTER_KUN_NAVN_MED_PDL_SOM_MASTER_UNDER_HENTPERSON = "/graphQLResponses/OnlyHentIdenterAktoerIdAndHentPersonNavnFromPDLMaster.json"
private const val KOMPLETT__HENTIDENTER_OG_HENTPERSON = "/graphQLResponses/Queryresponse.json"

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
        "Person to Proto - AllListsUnderHentPersonIsEmpty.json" {
            val proto = ((getStringFromResource(ALLE_ELEMENTER_UNDER_HENTPERSON_TOMME).getQueryResponseFromJsonString() as QueryResponse).toPerson() as PersonSf).toPersonProto()
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

        "Person to Proto - OnlyHentIdenterAktoerId.json" {
            val proto = ((getStringFromResource(KUN_AKTOERID_UNDER_HENTIDENTER_ALLE_ELEMENTER_UNDER_HENTPERSON_TOMME).getQueryResponseFromJsonString() as QueryResponse).toPerson() as PersonSf).toPersonProto()
            proto.first.aktoerId shouldBe AKTORID

            proto.second.identifikasjonsnummer shouldBe UKJENT_FRA_PDL
            proto.second.fornavn shouldBe UKJENT_FRA_PDL
            proto.second.mellomnavn shouldBe UKJENT_FRA_PDL
            proto.second.etternavn shouldBe UKJENT_FRA_PDL
            proto.second.adressebeskyttelse shouldBe PersonProto.PersonValue.Gradering.UGRADERT
            proto.second.sikkerhetstiltakList shouldBe emptyList<String>()
            proto.second.kommunenummer shouldBe UKJENT_FRA_PDL
            proto.second.region shouldBe UKJENT_FRA_PDL
            proto.second.doed shouldBe false
        }

        "Person to Proto - OnlyHentIdenterAktoerIdAndHentPersonNavnFromPDLMaster.json" {
            val proto = ((getStringFromResource(KUN_AKTOERID_UNDER_HENTIDENTER_KUN_NAVN_MED_PDL_SOM_MASTER_UNDER_HENTPERSON).getQueryResponseFromJsonString() as QueryResponse).toPerson() as PersonSf).toPersonProto()
            proto.first.aktoerId shouldBe AKTORID

            proto.second.identifikasjonsnummer shouldBe UKJENT_FRA_PDL
            proto.second.fornavn shouldBe "Gammel"
            proto.second.mellomnavn shouldBe ""
            proto.second.etternavn shouldBe "Hund"
            proto.second.adressebeskyttelse shouldBe PersonProto.PersonValue.Gradering.UGRADERT
            proto.second.sikkerhetstiltakList shouldBe emptyList<String>()
            proto.second.kommunenummer shouldBe UKJENT_FRA_PDL
            proto.second.region shouldBe UKJENT_FRA_PDL
            proto.second.doed shouldBe false
        }

        "Person to Proto - Queryresponse.json" {
            val proto = ((getStringFromResource(KOMPLETT__HENTIDENTER_OG_HENTPERSON).getQueryResponseFromJsonString() as QueryResponse).toPerson() as PersonSf).toPersonProto()
            proto.first.aktoerId shouldBe AKTORID

            proto.second.identifikasjonsnummer shouldBe FOLKEREGISTERIDENT
            proto.second.fornavn shouldBe "KUNNSKAPSLØS"
            proto.second.mellomnavn shouldBe ""
            proto.second.etternavn shouldBe "APRILSNARR"
            proto.second.adressebeskyttelse shouldBe PersonProto.PersonValue.Gradering.UGRADERT
            proto.second.sikkerhetstiltakList shouldBe haveSize<String>(2)
            proto.second.kommunenummer shouldBe "0301"
            proto.second.region shouldBe "03"
            proto.second.doed shouldBe false
        }
    }
}
*/
