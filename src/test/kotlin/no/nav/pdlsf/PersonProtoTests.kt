package no.nav.pdlsf

import io.kotlintest.matchers.asClue
import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.matchers.numerics.shouldBeExactly
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.UnstableDefault

private const val ONE = "/graphQLResponses/AllListsUnderHentPersonIsEmpty.json"

@OptIn(UnstableDefault::class)
@ImplicitReflectionSerializer
class PersonProtoTests : StringSpec() {

    init {

        "Three identical PersonValue messages should have identical hash code " {
            Person().toPersonProto().second.hashCode().let { hashCode ->
                (1..3).map { Person().toPersonProto().second.hashCode() }.toSet().asClue {
                    it.shouldHaveSize(1)
                    it.first() shouldBeExactly hashCode
                }
            }
        }

        "Three different PersonValue messages should have different hash code" {
            listOf(
                    Person(identifikasjonsnummer = "Torstein"),
                    Person(identifikasjonsnummer = "TorsteiN"),
                    Person(identifikasjonsnummer = "TorsTein")
            ).map { it.toPersonProto().second.hashCode() }.toSet() shouldHaveSize(3)
        }

        "Three different gradering should give different hash code" {
            listOf(
                    Person(adressebeskyttelse = Gradering.UGRADERT),
                    Person(adressebeskyttelse = Gradering.FORTROLIG),
                    Person(adressebeskyttelse = Gradering.STRENGT_FORTROLIG)
            ).map { it.toPersonProto().second.hashCode() }.toSet() shouldHaveSize(3)
        }
    }

    init {
        "Person to Proto - AllListsUnderHentPersonIsEmpty.json" {
            val proto = ((getStringFromResource(ONE).getQueryResponseFromJsonString() as QueryResponse).toPerson() as Person).toPersonProto()
            proto.second.fornavn shouldBe UKJENT_FRA_PDL
        }
    }
}
