
package org.fcrepo.server.storage.distributed;

import org.fcrepo.server.storage.DOReader;

public interface DeploymentManager {

    public String lookupDeployment(String cModelPid, String sDefPid);

    public void preCommit(DOReader reader);

    public void postCommit(DOReader reader);
}
