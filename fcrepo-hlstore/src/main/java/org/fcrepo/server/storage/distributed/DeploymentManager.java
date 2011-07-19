
package org.fcrepo.server.storage.distributed;


public interface DeploymentManager {

    public String lookupDeployment(String cModelPid, String sDefPid);

    public void addDeployment(String cModelPid,String sDefPid,String sDep);
}
