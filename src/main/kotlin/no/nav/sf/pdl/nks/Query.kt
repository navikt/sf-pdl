package no.nav.sf.pdl.nks

import java.time.LocalDate
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import no.nav.sf.library.jsonNonStrict

private val log = KotlinLogging.logger { }

fun String.getQueryFromJson(): QueryBase = runCatching {
    jsonNonStrict.parse(Query.serializer(), this)
}
        .onFailure {
            log.error { "Cannot convert kafka value to query - ${it.localizedMessage}" }
        }
        .getOrDefault(InvalidQuery)

@Serializable
enum class IdentGruppe {
    AKTORID,
    FOLKEREGISTERIDENT,
    NPID
}

@Serializable
enum class AdressebeskyttelseGradering {
    STRENGT_FORTROLIG_UTLAND,
    STRENGT_FORTROLIG,
    FORTROLIG,
    UGRADERT
}

@Serializable
enum class KjoennType {
    MANN,
    KVINNE,
    UKJENT
}

@Serializable
enum class GtType {
    KOMMUNE,
    BYDEL,
    UTLAND,
    UDEFINERT
}

@Serializable
enum class FamilieRelasjonsRolle {
    BARN,
    MOR,
    FAR,
    MEDMOR
}

@Serializable
enum class Sivilstandstype {
    UOPPGITT,
    UGIFT,
    GIFT,
    ENKE_ELLER_ENKEMANN,
    SKILT,
    SEPARERT,
    REGISTRERT_PARTNER,
    SEPARERT_PARTNER,
    SKILT_PARTNER,
    GJENLEVENDE_PARTNER
}

@Serializable
enum class Tiltakstype {
    FYUS,
    TFUS,
    FTUS,
    TOAN
}

@Serializable
data class Metadata(
    val historisk: Boolean = true,
    val master: String
)

sealed class QueryBase
object InvalidQuery : QueryBase()

@Serializable
data class Query(
    val hentPerson: HentePerson,
    val hentIdenter: Identliste
) : QueryBase()

@Serializable
data class Identliste(
    val identer: List<IdentInformasjon>
) {
    @Serializable
    data class IdentInformasjon(
        val ident: String,
        val historisk: Boolean,
        val gruppe: IdentGruppe
    )
}

@Serializable
data class HentePerson(
    val adressebeskyttelse: List<Adressebeskyttelse> = listOf(),
    val bostedsadresse: List<Bostedsadresse> = listOf(),
    val oppholdsadresse: List<Oppholdsadresse> = listOf(), // oppholdsadresse
    val doedsfall: List<Doedsfall> = listOf(),
    var familierelasjoner: List<FamilieRelasjon> = listOf(),
    var innflyttingTilNorge: List<InnflyttingTilNorge> = listOf(),
    val folkeregisterpersonstatus: List<Folkeregisterpersonstatus> = listOf(),
    val sikkerhetstiltak: List<Sikkerhetstiltak> = listOf(),
    var statsborgerskap: List<Statsborgerskap> = listOf(),
    val sivilstand: List<Sivilstand> = listOf(),
    val telefonnummer: List<Telefonnummer> = listOf(),
    val kjoenn: List<Kjoenn> = listOf(),
    val navn: List<Navn> = listOf(),
    val geografiskTilknytning: GeografiskTilknytning? = null,
    val utflyttingFraNorge: List<UtflyttingFraNorge> = listOf(),
    val tilrettelagtKommunikasjon: List<TilrettelagtKommunikasjon> = listOf(),
    val fullmakt: List<Fullmakt> = listOf(),
    val vergemaalEllerFremtidsfullmakt: List<VergemaalEllerFremtidsfullmakt> = listOf()
) {

    @Serializable
    data class Fullmakt(
        val motpartsPersonident: String? = null
    )

    @Serializable
    data class VergemaalEllerFremtidsfullmakt(
        val type: String? = null
    )

    @Serializable
    data class Bostedsadresse(
        val vegadresse: Vegadresse?,
        val matrikkeladresse: Matrikkeladresse?,
        val utenlandskAdresse: UtenlandskAdresse? = null,
        val ukjentBosted: UkjentBosted?,
        val metadata: Metadata
    ) {
        @Serializable
        data class Vegadresse(
            val kommunenummer: String?,
            val adressenavn: String?,
            val husnummer: String?,
            val husbokstav: String?,
            val postnummer: String?,
            val bydelsnummer: String? = null, // Data fields missing in preprod, this makes it optional
            val koordinater: Koordinater? = null
        )

        @Serializable
        data class Koordinater(
            val x: Float?,
            val y: Float?,
            val z: Float?
        )

        @Serializable
        data class Matrikkeladresse(
            val kommunenummer: String? = null,
            val postnummer: String? = null,
            val bydelsnummer: String? = null, // Data fields missing in preprod, this makes it optional
            val koordinater: Koordinater? = null
        )

        @Serializable
        data class UtenlandskAdresse(
            val adressenavnNummer: String?,
            val bygningEtasjeLeilighet: String?,
            val postboksNummerNavn: String?,
            val postkode: String?,
            val bySted: String?,
            val regionDistriktOmraade: String?,
            val landkode: String = ""
        )

        @Serializable
        data class UkjentBosted(
            val bostedskommune: String?
        )
    }

    @Serializable
    data class Oppholdsadresse(
        val vegadresse: Vegadresse? = null,
        val utenlandskAdresse: UtenlandskAdresse? = null, // utenlandskAdresse
        val matrikkeladresse: Matrikkeladresse? = null,
        val oppholdAnnetSted: String? = null,
        val metadata: Metadata
    ) {
        @Serializable
        data class Vegadresse(
            val kommunenummer: String? = null,
            val adressenavn: String? = null,
            val husnummer: String? = null,
            val husbokstav: String? = null,
            val postnummer: String? = null,
            val bydelsnummer: String? = null,
            val koordinater: Koordinater? = null
        )

        @Serializable
        data class Matrikkeladresse(
            val kommunenummer: String?,
            val postnummer: String?,
            val bydelsnummer: String? = null,
            val koordinater: Bostedsadresse.Koordinater?
        )

        @Serializable
        data class UtenlandskAdresse(
            val adressenavnNummer: String?,
            val bygningEtasjeLeilighet: String?,
            val postboksNummerNavn: String?,
            val postkode: String?,
            val bySted: String?,
            val regionDistriktOmraade: String?,
            val landkode: String?
        )

        @Serializable
        data class Koordinater(
            val x: Float? = null,
            val y: Float? = null,
            val z: Float? = null
        )
    }

    @Serializable
    data class Doedsfall(
        @Serializable(with = IsoLocalDateSerializer::class)
        val doedsdato: LocalDate?,
        val metadata: Metadata
    )

    @Serializable
    data class InnflyttingTilNorge(
        val fraflyttingsland: String?,
        val fraflyttingsstedIUtlandet: String?,
        val metadata: Metadata
    )

    @Serializable
    data class FamilieRelasjon(
        val relatertPersonsIdent: String = "",
        val relatertPersonsRolle: FamilieRelasjonsRolle,
        val minRolleForPerson: FamilieRelasjonsRolle?,
        val metadata: Metadata
    )

    @Serializable
    data class Kontaktperson(
        val personident: String?,
        val enhet: String?
    )

    @Serializable
    data class Sikkerhetstiltak(
        val tiltakstype: Tiltakstype,
        val beskrivelse: String,
        @Serializable(with = IsoLocalDateSerializer::class)
        val gyldigFraOgMed: LocalDate? = null,
        @Serializable(with = IsoLocalDateSerializer::class)
        val gyldigTilOgMed: LocalDate? = null,
        val kontaktperson: Kontaktperson? = null,
        val metadata: Metadata
    )

    @Serializable
    data class Navn(
        val fornavn: String,
        val mellomnavn: String?,
        val etternavn: String,
        val metadata: Metadata
    )

    @Serializable
    data class Kjoenn(
        val kjoenn: KjoennType,
        val metadata: Metadata
    )

    @Serializable
    data class Statsborgerskap(
        val land: String?,
        val metadata: Metadata
    )

    @Serializable
    data class Sivilstand(
        val type: Sivilstandstype,
        @Serializable(with = IsoLocalDateSerializer::class)
        val gyldigFraOgMed: LocalDate? = null,
        val relatertVedSivilstand: String?,
        val metadata: Metadata
    )

    @Serializable
    data class Adressebeskyttelse(
        val gradering: AdressebeskyttelseGradering,
        val metadata: Metadata
    )

    @Serializable
    data class Folkeregisterpersonstatus(
        val status: String,
        val metadata: Metadata
    )

    @Serializable
    data class GeografiskTilknytning(
        val gtType: GtType,
        val gtKommune: String?,
        val gtBydel: String?,
        val gtLand: String?,
        val metadata: Metadata
    )

    @Serializable
    data class UtflyttingFraNorge(
        val tilflyttingsland: String?,
        val tilflyttingsstedIUtlandet: String?,
        val metadata: Metadata
    )

    @Serializable
    data class TilrettelagtKommunikasjon(
        val talespraaktolk: Tolk?,
            // Ignoring PDL as source for tegnspraaktolk
        val metadata: Metadata
    )

    @Serializable
    data class Tolk(
        val spraak: String?
    )

    @Serializable
    data class Telefonnummer(
        val landskode: String,
        val nummer: String,
        val prioritet: Int,
        val metadata: Metadata
    )
}
