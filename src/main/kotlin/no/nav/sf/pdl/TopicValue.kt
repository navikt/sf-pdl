package no.nav.sf.pdl

import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.UnstableDefault
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import kotlinx.serialization.parse
import mu.KotlinLogging

private val log = KotlinLogging.logger { }

private val jsonNonStrict = Json(JsonConfiguration.Stable.copy(ignoreUnknownKeys = true, isLenient = true))

@UnstableDefault
@ImplicitReflectionSerializer
fun String.getQueryFromJson(): QueryBase = runCatching {
    jsonNonStrict.parse<Query>(this)
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
data class Metadata(
    val historisk: Boolean = true,
    val master: String
)

sealed class QueryBase
object InvalidQuery : QueryBase()

@Serializable
data class Query(
    val hentPerson: Person,
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
data class Person(
    val adressebeskyttelse: List<Adressebeskyttelse>,
    val bostedsadresse: List<Bostedsadresse>,
    val doedsfall: List<Doedsfall>,
    val sikkerhetstiltak: List<Sikkerhetstiltak>,
    val navn: List<Navn>

) {

    @Serializable
    data class Bostedsadresse(
        val vegadresse: Vegadresse?,
        val matrikkeladresse: Matrikkeladresse?,
        val ukjentBosted: UkjentBosted?,
        val metadata: Metadata
    ) {
        @Serializable
        data class Vegadresse(
            val kommunenummer: String?
        )

        @Serializable
        data class Matrikkeladresse(
            val kommunenummer: String?
        )

        @Serializable
        data class UkjentBosted(
            val bostedskommune: String?
        )
    }

    @Serializable
    data class Doedsfall(
        val metadata: Metadata
    )

    @Serializable
    data class Sikkerhetstiltak(
        val beskrivelse: String,
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
    data class Adressebeskyttelse(
        val gradering: AdressebeskyttelseGradering,
        val metadata: Metadata
    )
}

internal const val UKJENT_FRA_PDL = "<UKJENT_FRA_PDL>"
@ImplicitReflectionSerializer
fun Query.toPersonSf(): PersonBase {
    return runCatching {
        val kommunenummer = this.findKommunenummer()
        PersonSf(
                aktoerId = this.findAktoerId(),
                identifikasjonsnummer = this.findFolkeregisterIdent(),
                fornavn = this.findNavn().fornavn,
                mellomnavn = this.findNavn().mellomnavn,
                etternavn = this.findNavn().etternavn,
                adressebeskyttelse = this.findAdressebeskyttelse(),
                sikkerhetstiltak = this.hentPerson.sikkerhetstiltak.map { it.beskrivelse }.toList(),
                kommunenummer = kommunenummer,
                region = kommunenummer.regionOfKommuneNummer(),
                doed = this.hentPerson.doedsfall.isNotEmpty() // "doedsdato": null  betyr at han faktsik er død, man vet bare ikke når. Listen kan ha to innslagt, kilde FREG og PDL
        )
    }
            .onFailure { log.error { "Error creating PersonSf from Query ${it.localizedMessage}" } }
            .getOrDefault(PersonInvalid)
}

private fun Query.findAktoerId(): String {
    return this.hentIdenter.identer.let { it ->
        if (it.isEmpty()) {
            UKJENT_FRA_PDL
        } else {
            hentIdenter.identer.firstOrNull { it.gruppe == IdentGruppe.AKTORID }?.ident ?: UKJENT_FRA_PDL
        }
    }
}

private fun Query.findFolkeregisterIdent(): String {
    return this.hentIdenter.identer.let { it ->
        if (it.isEmpty()) {
            UKJENT_FRA_PDL
        } else {
            hentIdenter.identer.firstOrNull { it.gruppe == IdentGruppe.FOLKEREGISTERIDENT }?.ident ?: UKJENT_FRA_PDL
        }
    }
}
private fun Query.findAdressebeskyttelse(): AdressebeskyttelseGradering {
    return this.hentPerson.adressebeskyttelse.let { list ->
        if (list.isEmpty()) {
            AdressebeskyttelseGradering.UGRADERT
        } else {
            list.firstOrNull { !it.metadata.historisk }?.let { AdressebeskyttelseGradering.valueOf(it.gradering.name) } ?: AdressebeskyttelseGradering.UGRADERT
        }
    }
}

fun Query.findKommunenummer(): String {
    return this.hentPerson.bostedsadresse.let { bostedsadresse ->
        if (bostedsadresse.isNullOrEmpty()) {
            workMetrics.usedAddressTypes.labels(WMetrics.AddressType.INGEN.name).inc()
            UKJENT_FRA_PDL
        } else {
            bostedsadresse.firstOrNull { !it.metadata.historisk }?.let {
                it.vegadresse?.let { vegadresse ->
                    workMetrics.usedAddressTypes.labels(WMetrics.AddressType.VEGAADRESSE.name).inc()
                    vegadresse.kommunenummer
                } ?: it.matrikkeladresse?.let { matrikkeladresse ->
                    workMetrics.usedAddressTypes.labels(WMetrics.AddressType.MATRIKKELADRESSE.name).inc()
                    matrikkeladresse.kommunenummer
                } ?: it.ukjentBosted?.let { ukjentBosted ->
                    workMetrics.usedAddressTypes.labels(WMetrics.AddressType.UKJENTBOSTED.name).inc()
                    ukjentBosted.bostedskommune
                }
            } ?: UKJENT_FRA_PDL.also { workMetrics.usedAddressTypes.labels(WMetrics.AddressType.INGEN.name).inc() }
        }
    }
}

fun String.regionOfKommuneNummer(): String {
    return if (this == UKJENT_FRA_PDL) this else this.substring(0, 2)
}

fun Query.findNavn(): NavnBase {
    return if (this.hentPerson.navn.isNullOrEmpty()) {
        NavnBase.Ukjent()
    } else {
        this.hentPerson.navn.firstOrNull { it.metadata.master.toUpperCase() == "FREG" && !it.metadata.historisk }?.let {
            if (it.etternavn.isNotBlank() && it.fornavn.isNotBlank())
                NavnBase.Freg(
                        fornavn = it.fornavn,
                        etternavn = it.etternavn,
                        mellomnavn = it.mellomnavn.orEmpty()
                )
            else
                NavnBase.Ukjent(
                        fornavn = it.fornavn,
                        etternavn = it.etternavn,
                        mellomnavn = it.mellomnavn.orEmpty()
                )
        } ?: this.hentPerson.navn.firstOrNull { it.metadata.master.toUpperCase() == "PDL" && !it.metadata.historisk }?.let {
            if (it.etternavn.isNotBlank() && it.fornavn.isNotBlank())
                NavnBase.Pdl(
                        fornavn = it.fornavn,
                        etternavn = it.etternavn,
                        mellomnavn = it.mellomnavn.orEmpty()
                )
            else
                NavnBase.Ukjent(
                        fornavn = it.fornavn,
                        etternavn = it.etternavn,
                        mellomnavn = it.mellomnavn.orEmpty()
                )
        } ?: NavnBase.Ukjent()
    }
}

sealed class NavnBase {
    abstract val fornavn: String
    abstract val mellomnavn: String
    abstract val etternavn: String

    data class Freg(
        override val fornavn: String,
        override val mellomnavn: String,
        override val etternavn: String
    ) : NavnBase()

    data class Pdl(
        override val fornavn: String,
        override val mellomnavn: String,
        override val etternavn: String
    ) : NavnBase()

    data class Ukjent(
        override val fornavn: String = UKJENT_FRA_PDL,
        override val mellomnavn: String = UKJENT_FRA_PDL,
        override val etternavn: String = UKJENT_FRA_PDL
    ) : NavnBase()
}
