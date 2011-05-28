
package fedora.server.storage.distributed;

import fedora.server.storage.DOReader;

public interface DeploymentManager {

    public String lookupDeployment(String cModelPid, String sDefPid);

    public void preCommit(DOReader reader);

    public void postCommit(DOReader reader);
}
