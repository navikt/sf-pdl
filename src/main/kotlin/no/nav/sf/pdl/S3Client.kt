package no.nav.sf.pdl

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.CannedAccessControlList
import com.amazonaws.services.s3.model.CreateBucketRequest
import com.amazonaws.services.s3.model.PutObjectResult
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import java.io.File
import mu.KotlinLogging
import no.nav.sf.library.AVault
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.AnEnvironment.Companion.getEnvOrDefault

private val log = KotlinLogging.logger {}

const val EV_S3_REGION: String = "S3_REGION"

const val VAULT_S3_SECRET_KEY: String = "S3SecretKey"
const val VAULT_S3_ACCESS_KEY: String = "S3AccessKey"

const val SF_PDL_FILE = "filter.json"
const val SF_PDL_BUCKET = "sf-pdl-bucket"
object S3Client {

    private val s3: AmazonS3
    private val s3SecretKey = AVault.getSecretOrDefault(VAULT_S3_SECRET_KEY, "")
    private val s3AccessKey = AnEnvironment.getEnvOrDefault(VAULT_S3_SECRET_KEY, "")
    private val s3Region = AnEnvironment.getEnvOrDefault(EV_S3_REGION, "us-east-1")

    init {
        val s3Url = when (getEnvOrDefault("S3_INSTANCE", "LOCAL")) {
            "PRODUCTION" -> getEnvOrDefault("S3_URL", "")
            "PREPROD" -> getEnvOrDefault("S3_URL", "")
            else -> "http://localhost:8001"
        }
        val credentials = BasicAWSCredentials(s3AccessKey, s3SecretKey)
        log.info("New Client: (host: " + s3Url + " - " + s3Region + ", accesskey-length: " + s3AccessKey.length + "S3 secret key Length: " + s3SecretKey.length)
        s3 = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration(s3Url, s3Region))
                .enablePathStyleAccess()
                .withCredentials(AWSStaticCredentialsProvider(credentials))
                .build()
        createBucketIfMissing()
    }

    private fun createBucketIfMissing() {
        val bucketList = s3.listBuckets().filter { b -> b.name == SF_PDL_BUCKET }
        if (bucketList.isEmpty()) {
            log.info("Creating new bucket as its missing: $SF_PDL_BUCKET")
            s3.createBucket(CreateBucketRequest(SF_PDL_BUCKET).withCannedAcl(CannedAccessControlList.Private))
        }
        if (!s3.doesObjectExist(SF_PDL_BUCKET, SF_PDL_FILE)) {
            log.info("Creating empty file for persisting what have been pushed: $SF_PDL_FILE")
            s3.putObject(SF_PDL_BUCKET, SF_PDL_FILE, "")
        }
    }

    /**
     * Lagrer en filreferanse til S3
     */
    fun persistToS3(file: File): PutObjectResult {
        return s3.putObject(SF_PDL_BUCKET, SF_PDL_FILE, file)
    }

    private fun transferManager(): TransferManager {
        return TransferManagerBuilder.standard().withS3Client(s3).build()
    }

    /**
     * Laster object fra S3 og returnerer en filreferanse
     */
    fun loadFromS3(): File {
        val tempFile = createTempFile()
        transferManager()
                .download(SF_PDL_BUCKET, SF_PDL_FILE, tempFile)
                .waitForCompletion()
        return tempFile
    }

    fun hello() {
        log.info { "S3 client init done - hello" }
    }
}
