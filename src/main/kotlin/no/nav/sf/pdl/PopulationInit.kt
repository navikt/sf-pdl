package no.nav.sf.pdl

import java.io.File
import kotlin.streams.toList
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.KafkaConsumerStates
import no.nav.sf.library.send
import no.nav.sf.library.sendNullValue
import no.nav.sf.pdl.nks.HentePerson
import no.nav.sf.pdl.nks.InvalidQuery
import no.nav.sf.pdl.nks.Query
import no.nav.sf.pdl.nks.getQueryFromJson

private val log = KotlinLogging.logger {}

/*
internal fun parsePdlJson(cr: ConsumerRecord<String, String?>): PersonBase {
    if (cr.value() == null) {
        workMetrics.noOfInitialTombestone.inc()
        return PersonTombestone(aktoerId = cr.key())
    } else {
        when (val query = cr.value()?.getQueryFromJson() ?: InvalidQuery) {
            InvalidQuery -> {
                log.error { "Unable to parse topic value PDL" }
                workMetrics.invalidPersonsParsed.inc()
                return PersonInvalid
            }
            is Query -> {
                when (val personSf = query.toPersonSf()) {
                    is PersonSf -> {
                        workMetrics.noOfInitialPersonSf.inc()
                        return personSf
                    }
                    is PersonInvalid -> {
                        workMetrics.invalidPersonsParsed.inc()
                        return PersonInvalid
                    }
                    else -> {
                        log.error { "Unhandled PersonBase from Query.toPersonSf" }
                        workMetrics.invalidPersonsParsed.inc()
                        return PersonInvalid
                    }
                }
            }
        }
    }
}*/

fun List<Pair<String, PersonBase>>.isValid(): Boolean {
    return filterIsInstance<PersonInvalid>().isEmpty()
}

var heartBeatConsumer: Int = 0

val NOT_FOUND = "<NOT FOUND>"

private fun Query.findGtKommunenr(): String {
    val kommunenr: Kommunenummer = this.hentPerson.geografiskTilknytning?.let { gt ->
        when (gt.gtType) {
            GtType.KOMMUNE -> {
                if (gt.gtKommune.isNullOrEmpty()) {
                    Kommunenummer.Missing
                } else if ((gt.gtKommune.length == 4) || gt.gtKommune.all { c -> c.isDigit() }) {
                    Kommunenummer.Exist(gt.gtKommune)
                } else {
                    Kommunenummer.Invalid
                }
            }
            GtType.BYDEL -> {
                if (gt.gtBydel.isNullOrEmpty()) {
                    Kommunenummer.Missing
                } else if ((gt.gtBydel.length == 6) || gt.gtBydel.all { c -> c.isDigit() }) {
                    Kommunenummer.Exist(gt.gtBydel.substring(0, 4))
                } else {
                    Kommunenummer.Invalid
                }
            }
            GtType.UTLAND -> {
                Kommunenummer.GtUtland
            }
            GtType.UDEFINERT -> {
                Kommunenummer.GtUdefinert
            }
            else -> Kommunenummer.Missing
        }
    } ?: Kommunenummer.Missing

    return if (kommunenr is Kommunenummer.Exist)
        kommunenr.knummer
    else {
        UKJENT_FRA_PDL
    }
}

fun HentePerson.Bostedsadresse.Vegadresse.findKommuneNummer(): Kommunenummer {
    if (this.kommunenummer.isNullOrEmpty()) {
        return Kommunenummer.Missing
    } else if ((this.kommunenummer.length == 4) || this.kommunenummer.all { c -> c.isDigit() }) {
        return Kommunenummer.Exist(this.kommunenummer)
    } else {
        log.error { "Found invalid kommunenummer ${this.kommunenummer}" }
        return Kommunenummer.Invalid
    }
}

fun HentePerson.Bostedsadresse.Matrikkeladresse.findKommuneNummer(): Kommunenummer {
    if (this.kommunenummer.isNullOrEmpty()) {
        return Kommunenummer.Missing
    } else if ((this.kommunenummer.length == 4) || this.kommunenummer.all { c -> c.isDigit() }) {
        return Kommunenummer.Exist(this.kommunenummer)
    } else {
        log.error { "Found invalid kommunenummer ${this.kommunenummer}" }
        return Kommunenummer.Invalid
    }
}

fun HentePerson.Bostedsadresse.UkjentBosted.findKommuneNummer(): Kommunenummer {
    if (this.bostedskommune.isNullOrEmpty()) {
        return Kommunenummer.Missing
    } else if ((this.bostedskommune.length == 4) || this.bostedskommune.all { c -> c.isDigit() }) {
        return Kommunenummer.Exist(this.bostedskommune)
    } else {
        log.error { "Invalid kommunenr found: ${this.bostedskommune}" }
        return Kommunenummer.Invalid
    }
}

fun Query.findAdresseKommunenr(): String {
    return this.hentPerson.bostedsadresse.let { bostedsadresse ->
        if (bostedsadresse.isNullOrEmpty()) {
            UKJENT_FRA_PDL
        } else {
            bostedsadresse.firstOrNull { !it.metadata.historisk }?.let {
                it.vegadresse?.let { vegadresse ->
                    if (vegadresse.findKommuneNummer() is Kommunenummer.Exist) {
                        vegadresse.kommunenummer
                    } else null
                } ?: it.matrikkeladresse?.let { matrikkeladresse ->
                    if (matrikkeladresse.findKommuneNummer() is Kommunenummer.Exist) {
                        matrikkeladresse.kommunenummer
                    } else null
                } ?: it.ukjentBosted?.let { ukjentBosted ->
                    if (ukjentBosted.findKommuneNummer() is Kommunenummer.Exist) {
                        ukjentBosted.bostedskommune
                    } else null
                }
            } ?: UKJENT_FRA_PDL
        }
    }
}

class InvestigateGoal {
    var msgFailed: String = NOT_FOUND

    /*
    var msgWithoutGtKommuneNrButWithAdresseKommunenr: MutableList<String> = mutableListOf()

    var msgWithMoreThenOneAddressNotHistoric: MutableList<String> = mutableListOf()

    var msgWithBydelsnummer: MutableList<String> = mutableListOf()

    var msgWithHusnummer: MutableList<String> = mutableListOf()

     */

    // val targetMap: MutableMap<String, String> = mutableMapOf()
    var msgWithTarget1: MutableList<String> = mutableListOf()
    var msgWithTarget2: MutableList<String> = mutableListOf()
    var msgWithTarget3: MutableList<String> = mutableListOf()

    fun investigate(msg: String): Boolean {
        val queryBase: no.nav.sf.pdl.nks.QueryBase = msg.getQueryFromJson()

        if (queryBase is InvalidQuery) {
            msgFailed = msg
            return true // Done
        } else {
            var unAnswered = true // TODO Normally false to conclude when all is found - true forces to run through all
            val query = (queryBase as Query)

            /*
            if (query.hentIdenter.identer.any { targets.contains(it.ident) }) {
                val t = query.hentIdenter.identer.first { targets.contains(it.ident) }.ident
                targetMap[t] = msg
            }

             */

            if (msgWithTarget1.size < 1000) {
                if (query.findAdresseKommunenr() != UKJENT_FRA_PDL && query.findGtKommunenr() != UKJENT_FRA_PDL && query.findAdresseKommunenr() != query.findGtKommunenr()) {
                    msgWithTarget1.add(msg)
                    return false
                }
                unAnswered = true
            }
/*
            if (msgWithTarget2.size < 10) {
                if (query.hentPerson.vergemaalEllerFremtidsfullmakt.isNotEmpty()) {
                    msgWithTarget2.add(msg)
                    return false
                }
                unAnswered = true
            }

             */

            // if (query.hentIdenter.identer.any { it.ident == "16098625201" }) {
                // msgWithTarget3.add(msg)
            // }

            return !unAnswered // Done if there are no unanswered queries
        }
    }

    fun resultMsg(): String {
        // var result = "hits: ${targetMap.size} msgFailed:\n$msgFailed\n"
        // targetMap.forEach { result += "msgOf fnr ${it.key}:\n${it.value}\n" }
        var result = "\nNo of found ${msgWithTarget1.size}"
        msgWithTarget1.forEach { result += "\n\n$it" }

        return result
    }
}

fun no.nav.sf.pdl.nks.Query.findAdresseKommunenummer(): String {
    return this.hentPerson.bostedsadresse.let { bostedsadresse ->
        if (bostedsadresse.isNullOrEmpty()) {
            // workMetrics.usedAddressTypes.labels(WMetrics.AddressType.INGEN.name).inc()
            UKJENT_FRA_PDL
        } else {
            bostedsadresse.firstOrNull { !it.metadata.historisk }?.let {
                it.vegadresse?.let { vegadresse ->
                    if (vegadresse.kommunenummer?.isNotBlank() == true) {
                        // workMetrics.usedAddressTypes.labels(WMetrics.AddressType.VEGAADRESSE.name).inc()
                        vegadresse.kommunenummer
                    } else null
                } ?: it.matrikkeladresse?.let { matrikkeladresse ->
                    if (matrikkeladresse.kommunenummer?.isNotBlank() == true) {
                        // workMetrics.usedAddressTypes.labels(WMetrics.AddressType.MATRIKKELADRESSE.name).inc()
                        matrikkeladresse.kommunenummer
                    } else null
                } ?: it.ukjentBosted?.let { ukjentBosted ->
                    if (ukjentBosted.bostedskommune?.isNotBlank() == true) {
                        // workMetrics.usedAddressTypes.labels(WMetrics.AddressType.UKJENTBOSTED.name).inc()
                        ukjentBosted.bostedskommune
                    } else null
                }
            } ?: UKJENT_FRA_PDL // .also { workMetrics.usedAddressTypes.labels(WMetrics.AddressType.INGEN.name).inc() }
        }
    }
}

fun String.watchFor(msg: String, match: String, times: Int = 1): String {
    if (this == NOT_FOUND) {
        if (msg.contains(match, true)) {
            if (times == 1) {
                return msg
            }
        }
    }
    return this
}

internal fun initLoadTest(ws: WorkSettings) {
    workMetrics.clearAll()
    val kafkaConsumerPdlTest = AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerPdlAlternative,
            topics = listOf(kafkaPDLTopic),
            fromBeginning = true
    )

    val resultListTest: MutableList<String> = mutableListOf()

    val resultListInvestigate = mutableListOf<String>()

    val investigateGoal = InvestigateGoal()

    kafkaConsumerPdlTest.consume { cRecords ->
        if (cRecords.isEmpty) return@consume KafkaConsumerStates.IsFinished

        workMetrics.initRecordsParsedTest.inc(cRecords.count().toDouble())
        cRecords.forEach { cr -> resultListTest.add(cr.key())
            if (cr.value() != null) {
                val done = investigateGoal.investigate(cr.value() as String)
                if (done) {
                    return@consume KafkaConsumerStates.IsFinished
                }
            }
        }

        /*
        cRecords.filter { cr -> target.any { it == cr.key() } }.forEach {
            log.info { "Investigate - found target" }
            resultListInvestigate.add(it.value() ?: "null")
        }
         */

        if (heartBeatConsumer == 0) {
            log.debug { "Investigate Test phase Successfully consumed a batch (This is prompted 100000th consume batch)" }
            /*

            if (cRecords.count() > 9) { // TODO investigate
                cRecords.forEachIndexed { index, consumerRecord -> if (index < 10) {
                    a[index] = consumerRecord.value()
                } }
            }

             */
        }

        heartBeatConsumer = ((heartBeatConsumer + 1) % 100000)
        KafkaConsumerStates.IsOk
    }
    heartBeatConsumer = 0

    // TODO investigate
    var msg = "Msgs nks\n"
    // a.forEach { str -> msg += str + "\n" }
    resultListInvestigate.forEach { str -> msg += str + "\n" }
    log.info { "Attempt file storage new" }
    File("/tmp/investigate").writeText(investigateGoal.resultMsg())
    log.info { "File storage done" }

    log.info { "Init test run : Total records from topic: ${resultListTest.size}" }
    workMetrics.initRecordsParsedTest.set(resultListTest.size.toDouble())
    log.info { "Init test run : Total unique records from topic: ${resultListTest.stream().distinct().toList().size}" }
    heartBeatConsumer = 0
}

internal fun initLoad(ws: WorkSettings): ExitReason {
    // initLoadTest(ws)

    workMetrics.clearAll()

    if (ws.filter is FilterBase.Missing) {
        log.warn { "initLoad - No filter for activities, leaving" }
        return ExitReason.NoFilter
    }
    val filter = ws.filter as FilterBase.Exists

    log.info { "Commencing reading all records on topic $kafkaPDLTopic" }

    val resultList: MutableList<Pair<String, PersonBase>> = mutableListOf()

    val kafkaConsumerPdl = AKafkaConsumer<String, String?>(
            config = ws.kafkaConsumerPdl,
            topics = listOf(kafkaPDLTopic),
            fromBeginning = true
    )
/*
    kafkaConsumerPdl.consume { cRecords ->
        if (cRecords.isEmpty) return@consume KafkaConsumerStates.IsFinished

        workMetrics.initRecordsParsed.inc(cRecords.count().toDouble())
        cRecords.forEach { cr -> resultList.add(Pair(cr.key(), parsePdlJson(cr))) }
        if (heartBeatConsumer == 0) {
            log.debug { "Successfully consumed a batch (This is prompted 100000th consume batch)" }
        }
        heartBeatConsumer = ((heartBeatConsumer + 1) % 100000)

        KafkaConsumerStates.IsOk
    }

 */

    if (!resultList.isValid()) {
        log.error { "Result contains invalid records" }
        return ExitReason.InvalidCache
    }

    log.info { "Number of records from topic $kafkaPDLTopic is ${resultList.size}" }

    val latestRecords = resultList.toMap() // Remove duplicates

    log.info { "Number of unique aktoersIds (and corresponding messages to handle) on topic $kafkaPDLTopic is ${latestRecords.size}" }

    val numberOfDeadPeopleFound = latestRecords.filter { r -> r.value is PersonSf && (r.value as PersonSf).doed }.size

    log.info { "Number of aktoersIds that is dead people $numberOfDeadPeopleFound" }
    workMetrics.deadPersons.set(numberOfDeadPeopleFound.toDouble())

    val filteredRecords = latestRecords.filter { r -> r.value is PersonTombestone ||
            (r.value is PersonSf &&
                    !(r.value as PersonSf).doed &&
                    (!ws.filterEnabled || filter.approved(r.value as PersonSf, true)))
    }.map {
        if (it.value is PersonSf) {
            (it.value as PersonSf).measureKommune()
            if (it.key == "1000025964669") { log.info { "Found reference person in filtering" } }
            (it.value as PersonSf).toPersonProto()
        } else {
            Pair<PersonProto.PersonKey, PersonProto.PersonValue?>((it.value as PersonTombestone).toPersonTombstoneProtoKey(), null)
        }
    }.toList().asSequence()

    log.info { "Number of records filtered, translated and ready to send ${filteredRecords.count().toDouble()}" }
    workMetrics.noOfInitialKakfaRecordsPdl.set(filteredRecords.count().toDouble())

    var exitReason: ExitReason = ExitReason.NoKafkaProducer

    var produceCount: Int = 0

    filteredRecords.chunked(500000).forEach {
        log.info { "Creating producer for batch ${produceCount++}" }
        AKafkaProducer<ByteArray, ByteArray>(
                config = ws.kafkaProducerPerson
        ).produce {
            it.fold(true) { acc, pair ->
                acc && pair.second?.let {
                    send(kafkaPersonTopic, pair.first.toByteArray(), it.toByteArray()).also { workMetrics.initiallyPublishedPersons.inc() }
                } ?: sendNullValue(kafkaPersonTopic, pair.first.toByteArray()).also { workMetrics.initiallyPublishedTombestones.inc() }
            }.let { sent ->
                when (sent) {
                    true -> exitReason = ExitReason.Work
                    false -> {
                        exitReason = ExitReason.InvalidCache
                        workMetrics.producerIssues.inc()
                        log.error { "Init load - Producer $produceCount has issues sending to topic" }
                    }
                }
            }
        }
    }

    log.info { "Init load - Done with publishing to topic exitReason is Ok? ${exitReason.isOK()} " }

    return exitReason
}

fun PersonSf.measureKommune() {
    val kommuneLabel = if (this.kommunenummer == UKJENT_FRA_PDL) {
        UKJENT_FRA_PDL
    } else {
        PostnummerService.getKommunenummer(this.kommunenummer)?.let {
            it
        } ?: workMetrics.kommune_number_not_found.labels(this.kommunenummer).inc().let { NOT_FOUND_IN_REGISTER }
    }
    workMetrics.kommune.labels(kommuneLabel).inc()
}

fun <T> Sequence<T>.batch(n: Int): Sequence<List<T>> {
    return BatchingSequence(this, n)
}

private class BatchingSequence<T>(val source: Sequence<T>, val batchSize: Int) : Sequence<List<T>> {
    override fun iterator(): Iterator<List<T>> = object : AbstractIterator<List<T>>() {
        val iterate = if (batchSize > 0) source.iterator() else emptyList<T>().iterator()
        override fun computeNext() {
            if (iterate.hasNext()) setNext(iterate.asSequence().take(batchSize).toList())
            else done()
        }
    }
}
