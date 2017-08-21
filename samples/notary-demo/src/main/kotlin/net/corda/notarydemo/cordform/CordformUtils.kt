package net.corda.notarydemo.cordform

import net.corda.cordform.CordformNode
import net.corda.core.node.services.ServiceInfo

/**
 * @return whether a CordaformNode runs a notary.
 */
fun CordformNode.isNotary() : Boolean {
    return this.advertisedServices
            .map { service -> ServiceInfo.parse(service) }
            .any { serviceInfo -> serviceInfo.type.isNotary() }
}