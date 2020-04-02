package no.nav.pdlsf

import no.nav.common.KafkaEnvironment
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType

// utility function for setting access control list
internal fun KafkaEnvironment.addProducerToTopic(username: String, topic: String) = this.let { ke ->
    ke.adminClient?.createAcls(
        listOf(AclOperation.DESCRIBE, AclOperation.WRITE, AclOperation.CREATE)
            .map { aclOp ->
                AclBinding(
                    ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                    AccessControlEntry("User:$username", "*", aclOp, AclPermissionType.ALLOW)
                )
            }
    )
    ke
}

internal fun KafkaEnvironment.addConsumerToTopic(username: String, topic: String) = this.let { ke ->
    ke.adminClient?.createAcls(
        listOf(AclOperation.DESCRIBE, AclOperation.READ)
            .map { aclOp ->
                AclBinding(
                    ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                    AccessControlEntry("User:$username", "*", aclOp, AclPermissionType.ALLOW)
                )
            }
    )
    ke
}

internal fun KafkaEnvironment.setLogCompaction(topicName: String) = this.let { ke ->
    val configResource = ConfigResource(ConfigResource.Type.TOPIC, topicName)
    val configLogCompact = listOf(
            AlterConfigOp(
                    ConfigEntry("cleanup.policy", "compact"),
                    AlterConfigOp.OpType.SET
            )
    )
    val configReq: Map<ConfigResource, Collection<AlterConfigOp>> = mapOf(configResource to configLogCompact)

    ke.adminClient?.incrementalAlterConfigs(configReq)
    ke
}
