package org.fcrepo.server.storage.highlevel.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseAdminTool {
	private static final Logger log = LoggerFactory.getLogger(HBaseAdmin.class);

	private final HBaseAdmin admin;

	public HBaseAdminTool(HadoopProperties properties) throws IOException {
		admin = new HBaseAdmin(properties.getConfiguration());
	}

	public HTable createTable(String tableName, List<String> columns) throws IOException {
		log.debug("creating table " + tableName);
		HTableDescriptor t = new HTableDescriptor(Bytes.toBytes(tableName));
		for (String c : columns) {
			HColumnDescriptor cd = new HColumnDescriptor(c);
			t.addFamily(cd);
		}
		admin.createTable(t);
		return new HTable(tableName);
	}

	public boolean tableExists(String tableName) throws IOException {
		return admin.tableExists(Bytes.toBytes(tableName));
	}
}
