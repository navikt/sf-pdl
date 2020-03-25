package no.nav.pdlsf

import java.io.File
import java.io.FileNotFoundException
import java.util.Base64

object ParamsFactory {
    val p: Params by lazy { Params() }
}

// TODO:: Read parameters from vault
data class Params(
        // kafka details
    val kafkaBrokers: String = System.getenv("KAFKA_BROKERS")?.toString() ?: "",
    val kafkaSchemaRegistry: String = System.getenv("KAFKA_SCREG")?.toString() ?: "",
    val kafkaClientID: String = System.getenv("KAFKA_CLIENTID")?.toString() ?: "",
    val kafkaProducerTimeout: Int = System.getenv("KAFKA_PRODUCERTIMEOUT")?.toInt() ?: 31_000,
    val kafkaSecurity: String = System.getenv("KAFKA_SECURITY")?.toString()?.toUpperCase() ?: "",
    val kafkaSecProt: String = System.getenv("KAFKA_SECPROT")?.toString() ?: "",
    val kafkaSaslMec: String = System.getenv("KAFKA_SASLMEC")?.toString() ?: "",
    val kafkaUser: String = ("/var/run/secrets/nais.io/serviceuser/username".readFile() ?: "username"),
    val kafkaPassword: String = ("/var/run/secrets/nais.io/serviceuser/password".readFile() ?: "password"),
    val kafkaTopicPdl: String = System.getenv("KAFKA_TOPIC_PDL")?.toString() ?: "",
    val kafkaTopicSf: String = System.getenv("KAFKA_TOPIC_SF")?.toString() ?: "",

        // other details
    val httpsProxy: String = System.getenv("HTTPS_PROXY") ?: "",
    val msBetweenWork: Long = System.getenv("MS_BETWEEN_WORK")?.toLong() ?: 30 * 60 * 1_000 * 15,
    val pdlGraphQlApiKey: String = ("/var/run/secrets/nais.io/apigw/pdl-api/x-nav-apiKey".readFile() ?: ""),
    val pdlGraphQlUrl: String = System.getenv("PDL_GRAPHQL_URL") ?: "",
    val stsApiKey: String = ("/var/run/secrets/nais.io/apigw/security-token-service-token/x-nav-apiKey".readFile() ?: ""),
    val stsUrl: String = System.getenv("STS_REST_URL") ?: ""

)

fun Params.credentials(): String = Base64.getEncoder().encodeToString("$kafkaUser:$kafkaPassword".toByteArray(Charsets.UTF_8))

fun Params.kafkaSecurityEnabled(): Boolean = kafkaSecurity == "TRUE"

fun Params.kafkaSecurityComplete(): Boolean =
        kafkaSecProt.isNotEmpty() && kafkaSaslMec.isNotEmpty() && kafkaUser.isNotEmpty() && kafkaPassword.isNotEmpty()

internal fun String.readFile(): String? =
        try {
            File(this).readText(Charsets.UTF_8)
        } catch (err: FileNotFoundException) {
            null
        }

internal fun getStringFromResource(path: String) =
        ParamsFactory::class.java.getResourceAsStream(path).bufferedReader().use { it.readText() }
