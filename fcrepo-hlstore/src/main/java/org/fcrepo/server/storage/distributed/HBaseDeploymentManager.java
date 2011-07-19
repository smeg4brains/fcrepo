package org.fcrepo.server.storage.distributed;

import java.util.HashMap;
import java.util.Map;

public class HBaseDeploymentManager implements DeploymentManager {
	Map<String, String> modelDeploymentMap=new HashMap<String, String>();

	@Override
	public void addDeployment(String cModelPid,String sDefPid,String sDep){
		modelDeploymentMap.put(cModelPid + "~" + sDefPid, sDep);
	}
	
	@Override
	public String lookupDeployment(String cModelPid, String sDefPid) {
		String key=cModelPid + "~" + sDefPid;
		if (modelDeploymentMap.containsKey(key)){
			return modelDeploymentMap.get(key);
		}
		return null;
	}

}
