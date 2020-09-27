package no.nav.sf.pdl

import io.prometheus.client.Gauge

data class WMetrics(
    val noOfKakfaRecordsPdl: Gauge = Gauge
            .build()
            .name("no_kafkarecords_pdl_gauge")
            .help("No. of kafka records pdl")
            .register(),
    val noOfInitialKakfaRecordsPdl: Gauge = Gauge
            .build()
            .name("no_initial_kafkarecords_pdl_gauge")
            .help("No. of kafka records pdl")
            .register(),
    val noOfInitialTombestone: Gauge = Gauge
            .build()
            .name("no_initial_tombestones")
            .help("No. of kafka records pdl")
            .register(),

    val noOfInitialPersonSf: Gauge = Gauge
            .build()
            .name("no_initial_parsed_persons")
            .help("No. of parsed person sf")
            .register(),

    val noOfTombestone: Gauge = Gauge
            .build()
            .name("no_tombestones")
            .help("No. of kafka records pdl")
            .register(),

    val noOfPersonSf: Gauge = Gauge
            .build()
            .name("no_parsed_persons")
            .help("No. of parsed person sf")
            .register(),

    val sizeOfCache: Gauge = Gauge
            .build()
            .name("size_of_cache")
            .help("Size of person cache")
            .register(),

    val usedAddressTypes: Gauge = Gauge
            .build()
            .name("used_address_gauge")
            .labelNames("type")
            .help("No. of address types used in last work session")
            .register(),
    val initiallyPublishedPersons: Gauge = Gauge
            .build()
            .name("initially_published_person_gauge")
            .help("No. of persons published to kafka in last work session")
            .register(),
    val publishedPersons: Gauge = Gauge
            .build()
            .name("published_person_gauge")
            .help("No. of persons published to kafka in last work session")
            .register(),
    val initiallyPublishedTombestones: Gauge = Gauge
            .build()
            .name("initially_published_tombestone_gauge")
            .help("No. of persons published to kafka in last work session")
            .register(),
    val publishedTombestones: Gauge = Gauge
            .build()
            .name("published_tombestone_gauge")
            .help("No. of tombestones published to kafka in last work session")
            .register(),
    val cacheIsNewOrUpdated_noKey: Gauge = Gauge
            .build()
            .name("cache_no_key")
            .help("cache no key")
            .register(),
    val cacheIsNewOrUpdated_differentHash: Gauge = Gauge
            .build()
            .name("cache_different_hash")
            .help("cache different hash")
            .register(),
    val cacheIsNewOrUpdated_existing_to_tombestone: Gauge = Gauge
            .build()
            .name("cache_existing_to")
            .help("cache existing to")
            .register(),
    val cacheIsNewOrUpdated_no_blocked: Gauge = Gauge
            .build()
            .name("cache_no_blocked")
            .help("cache no blocked")
            .register(),
    val filterApproved: Gauge = Gauge
            .build()
            .name("filter_approved")
            .help("filter approved")
            .register(),
    val filterDisproved: Gauge = Gauge
            .build()
            .name("filter_disproved")
            .help("filter disproved")
            .register(),
    val initialFilterApproved: Gauge = Gauge
            .build()
            .name("initial_filter_approved")
            .help("filter approved")
            .register(),
    val initialFilterDisproved: Gauge = Gauge
            .build()
            .name("initial_filter_disproved")
            .help("filter disproved")
            .register(),
    val consumerIssues: Gauge = Gauge
            .build()
            .name("consumer_issues")
            .help("consumer issues")
            .register(),
    val producerIssues: Gauge = Gauge
            .build()
            .name("producer_issues")
            .help("producer issues")
            .register(),
    val latestInitBatch: Gauge = Gauge
            .build()
            .name("latest_init_batch")
            .help("latest init batch")
            .register(),
    val initRecordsParsed: Gauge = Gauge
            .build()
            .name("init_records_parsed")
            .help("init_records_parsed")
            .register(),
    val initRecordsParsedTest: Gauge = Gauge
            .build()
            .name("init_records_parsed_test")
            .help("init_records_parsed_test")
            .register(),
    val noInvalidKommuneNummer: Gauge = Gauge
            .build()
            .name("no_invalid_kommunenummer")
            .help("no_invalid_kommunenummer")
            .register(),
    val invalidKommuneNummer: Gauge = Gauge
            .build()
            .name("invalid_kommunenummer")
            .labelNames("kommunenummer")
            .help("invalid_kommunenummer")
            .register(),
// GT metrics start
    val gtKommunenrFraKommuneMissing: Gauge = Gauge
            .build()
            .name("gt_kommunenr_fra_kommune_missing")
            .help("gt_kommunenr_fra_kommune_missing")
            .register(),
    val gtKommunenrFraKommune: Gauge = Gauge
            .build()
            .name("gt_kommunenr_fra_kommune")
            .help("gt_kommunenr_fra_kommune")
            .register(),
    val gtKommuneInvalid: Gauge = Gauge
            .build()
            .name("gt_kommunenr_invalid")
            .help("gt_kommunenr_invalid")
            .register(),
    val gtKommunenrFraBydelMissing: Gauge = Gauge
            .build()
            .name("gt_kommunenr_fra_bydel_missing")
            .help("gt_kommunenr_fra_bydel_missing")
            .register(),
    val gtKommunenrFraBydel: Gauge = Gauge
            .build()
            .name("gt_kommunenr_fra_bydel")
            .help("gt_kommunenr_fra_bydel")
            .register(),
    val gtBydelInvalid: Gauge = Gauge
            .build()
            .name("gt_kommunenr_fra_bydel_invalid")
            .help("gt_kommunenr_fra_bydel_invalid")
            .register(),
    val gtUtland: Gauge = Gauge
            .build()
            .name("gt_kommunenr_gttype_utland")
            .help("gt_kommunenr_gttype_utland")
            .register(),
    val gtUdefinert: Gauge = Gauge
            .build()
            .name("gt_kommunenr_gttype_udefinert")
            .help("gt_kommunenr_gttype_udefinert")
            .register(),
    val gtMissing: Gauge = Gauge
            .build()
            .name("gt_missing")
            .help("gt_missing")
            .register(),

// GT metrics end
    val kommune: Gauge = Gauge
            .build()
            .name("kommune")
            .labelNames("kommune")
            .help("kommune")
            .register(),
    val kommune_number_not_found: Gauge = Gauge
            .build()
            .name("kommune_number_not_found")
            .labelNames("kommune_number")
            .help("kommune_number_not_found")
            .register(),
    val deadPersons: Gauge = Gauge
            .build()
            .name("dead_persons")
            .help("dead_persons")
            .register(),
    val lastCharParsed: Gauge = Gauge
            .build()
            .name("last_char_parsed")
            .labelNames("char_int")
            .help("last_char_parsed")
            .register(),
    val invalidPersonsParsed: Gauge = Gauge
            .build()
            .name("invalid_persons_parsed")
            .help("invalid_persons_parsed")
            .register()
) {
    enum class AddressType {
        VEGAADRESSE, MATRIKKELADRESSE, UKJENTBOSTED, INGEN
    }

    fun clearAll() {
        this.initRecordsParsedTest.clear()
        this.deadPersons.clear()
        this.lastCharParsed.clear()
        this.invalidPersonsParsed.clear()

        this.kommune.clear()
        this.gtKommunenrFraKommuneMissing.clear()
        this.gtKommunenrFraKommune.clear()
        this.gtKommuneInvalid.clear()
        this.gtKommunenrFraBydelMissing.clear()
        this.gtKommunenrFraBydel.clear()
        this.gtBydelInvalid.clear()
        this.gtUtland.clear()
        this.gtUdefinert.clear()
        this.gtMissing.clear()

        this.noInvalidKommuneNummer.clear()
        this.invalidKommuneNummer.clear()
        this.initRecordsParsed.clear()
        this.latestInitBatch.clear()
        this.noOfPersonSf.clear()
        this.noOfTombestone.clear()
        this.noOfKakfaRecordsPdl.clear()
        this.noOfInitialKakfaRecordsPdl.clear()
        this.noOfInitialPersonSf.clear()
        this.noOfInitialTombestone.clear()
        this.sizeOfCache.clear()
        this.usedAddressTypes.clear()
        this.publishedPersons.clear()
        this.publishedTombestones.clear()
        this.initiallyPublishedPersons.clear()
        this.initiallyPublishedTombestones.clear()
        this.cacheIsNewOrUpdated_differentHash.clear()
        this.cacheIsNewOrUpdated_existing_to_tombestone.clear()
        this.cacheIsNewOrUpdated_noKey.clear()
        this.cacheIsNewOrUpdated_no_blocked.clear()
        this.filterApproved.clear()
        this.filterDisproved.clear()
        this.initialFilterApproved.clear()
        this.initialFilterDisproved.clear()
        this.consumerIssues.clear()
        this.producerIssues.clear()
    }
}
