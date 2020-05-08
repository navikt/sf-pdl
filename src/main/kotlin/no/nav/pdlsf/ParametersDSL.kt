package no.nav.pdlsf

import java.io.File
import mu.KotlinLogging

private val log = KotlinLogging.logger { }

/**
 * data class Vault contains expected vault configuration
 * data class EnvVar contains expected environment variables
 *
 * data class Params contains the previous ones and a set of extension function
 */

const val PROGNAME = "sf-pdl"

sealed class IntegrityBase
data class IntegrityIssue(val cause: String) : IntegrityBase()
object IntegrityOk : IntegrityBase()

data class Params(
    val vault: Vault = Vault(),
    val envVar: EnvVar = EnvVar()
) {
    private fun kafkaSecurityConfigOk(): Boolean =
            envVar.kSecProt.isNotEmpty() && envVar.kSaslMec.isNotEmpty() &&
                    vault.kafkaUser.isNotEmpty() && vault.kafkaPassword.isNotEmpty()

    private fun kafkaBaseConfigOk(): Boolean =
            envVar.kBrokers.isNotEmpty() && envVar.kClientID.isNotEmpty() && envVar.kTopicPdl.isNotEmpty() && envVar.kTopicSf.isNotEmpty()

    fun integrityCheck(): IntegrityBase =
            when {
                !kafkaBaseConfigOk() -> IntegrityIssue("Kafka base config is incomplete")
                envVar.kSecurityEnabled && !kafkaSecurityConfigOk() ->
                    IntegrityIssue("Kafka security enabled, but incomplete kafka security properties")
                else -> IntegrityOk
            }
}

// according to dev and prod.yaml
const val pathSecrets = "/var/run/secrets/nais.io/vault/"
const val pathServiceUser = "/var/run/secrets/nais.io/serviceuser/"

data class Vault(
        // kafka details
    val kafkaUser: String = getServiceUserOrDefault("username", "kafkauser"),
    val kafkaPassword: String = getServiceUserOrDefault("password", "kafkapassword")
) {
    companion object {
        private fun getOrDefault(file: File, d: String): String = runCatching { file.readText(Charsets.UTF_8) }
                .onFailure { log.error { "Couldn't read ${file.absolutePath}" } }
                .getOrDefault(d)

        fun getSecretOrDefault(k: String, d: String = "", p: String = pathSecrets): String =
                getOrDefault(File("$p$k"), d)

        fun getServiceUserOrDefault(k: String, d: String = "", p: String = pathServiceUser): String =
                getOrDefault(File("$p$k"), d)
    }
}

data class EnvVar(

        // kafka details
    val kBrokers: String = getEnvOrDefault("KAFKA_BROKERS", "localhost:9092"),
    val kSchemaRegistry: String = getEnvOrDefault("KAFKA_SCREG"),
    val kClientID: String = getEnvOrDefault("KAFKA_CLIENTID", PROGNAME),
    val kSecurityEnabled: Boolean = getEnvOrDefault("KAFKA_SECURITY", "false").toBoolean(),
    val kSecProt: String = getEnvOrDefault("KAFKA_SECPROT"),
    val kSaslMec: String = getEnvOrDefault("KAFKA_SASLMEC"),
    val kTopicPdl: String = getEnvOrDefault("KAFKA_TOPIC_PDL", "default-topic-pdl"),
    val kTopicSf: String = getEnvOrDefault("KAFKA_TOPIC_SF", "default-topic-sf"),
    val kProducerTimeout: Int = System.getenv("KAFKA_PRODUCERTIMEOUT")?.toInt() ?: 31_000,

        // other
    val httpsProxy: String = getEnvOrDefault("HTTPS_PROXY"),
    val msBetweenWork: Long = getEnvOrDefault("MS_BETWEEN_WORK", "60000").toLong()
) {
    companion object {
        fun getEnvOrDefault(k: String, d: String = ""): String = runCatching { System.getenv(k) ?: d }.getOrDefault(d)
    }
}

internal fun String.getResourceOrDefault(d: String = ""): String =
        runCatching { Params::class.java.getResourceAsStream("/$this").bufferedReader().use { it.readText() } }
                .getOrDefault(d)

internal fun getStringFromResource(path: String) =
        Params::class.java.getResourceAsStream(path).bufferedReader().use { it.readText() }
