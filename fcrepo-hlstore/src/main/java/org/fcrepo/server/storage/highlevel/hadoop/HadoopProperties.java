package org.fcrepo.server.storage.highlevel.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.fcrepo.common.Constants;

public class HadoopProperties {
	private final Configuration configuration;
	private final String qualifier = "1";
	private final String format = Constants.FOXML1_1.toString();
	private final String encoding = "UTF-8";
	
	private String objectTableName;
	private String datastreamTableName;
	private String pidTableName;
	private String reservedPidsTableName;

	public HadoopProperties(Configuration configuration,String charsetName) {
		this.configuration = configuration;
	}

	public String getObjectTableName() {
		return objectTableName;
	}

	public void setObjectTableName(String objectTableName) {
		this.objectTableName = objectTableName;
	}

	public String getDatastreamTableName() {
		return datastreamTableName;
	}

	public void setDatastreamTableName(String datastreamTableName) {
		this.datastreamTableName = datastreamTableName;
	}

	public String getPidTableName() {
		return pidTableName;
	}

	public void setPidTableName(String pidTableName) {
		this.pidTableName = pidTableName;
	}

	public String getReservedPidsTableName() {
		return reservedPidsTableName;
	}

	public void setReservedPidsTableName(String reservedPidsTableName) {
		this.reservedPidsTableName = reservedPidsTableName;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public byte[] getQualifier() {
		return Bytes.toBytes(qualifier);
	}

	public byte[] getFormat() {
		return Bytes.toBytes(format);
	}

	public byte[] getEncoding() {
		return Bytes.toBytes(encoding);
	}

}
