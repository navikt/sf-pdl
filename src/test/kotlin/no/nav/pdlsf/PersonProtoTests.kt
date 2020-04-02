package no.nav.pdlsf

import io.kotlintest.matchers.asClue
import io.kotlintest.matchers.collections.shouldHaveSize
import io.kotlintest.matchers.numerics.shouldBeExactly
import io.kotlintest.specs.StringSpec
import no.nav.pdlsf.proto.PersonProto

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
                    Person(adressebeskyttelse = PersonProto.PersonValue.Gradering.UGRADERT),
                    Person(adressebeskyttelse = PersonProto.PersonValue.Gradering.FORTROLIG),
                    Person(adressebeskyttelse = PersonProto.PersonValue.Gradering.STRENGT_FORTROLIG)
            ).map { it.toPersonProto().second.hashCode() }.toSet() shouldHaveSize(3)
        }
    }
}
