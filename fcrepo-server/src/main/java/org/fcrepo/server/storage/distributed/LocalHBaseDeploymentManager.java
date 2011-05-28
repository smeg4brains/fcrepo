
package fedora.server.storage.distributed;

import fedora.server.storage.DOReader;

/**
 * Simple DeploymentManager operating isolated in single Fedora instance.
 * <p>
 * It is GENERALLY UNSAFE to use this deployment manager in situations where
 * there may be multiple writers. Specifically, if an unrelated writer changes
 * the deployment object for a particular model, instance will have no way of
 * discovering the change and will continue using the old deployment as long as
 * it continues to function. If the old deployment is rendered inoperable, or
 * this Fedora instance is re-started, the manager will be able to discover the
 * proper new value.
 * </p>
 */
public class LocalHBaseDeploymentManager
        implements DeploymentManager {

    public String lookupDeployment(String cModelPid, String sDefPid) {
        // TODO implement
        return null;
    }

    public void postCommit(DOReader reader) {
        // TODO Auto-generated method stub

    }

    public void preCommit(DOReader reader) {
        /* do nothing */
    }

}
