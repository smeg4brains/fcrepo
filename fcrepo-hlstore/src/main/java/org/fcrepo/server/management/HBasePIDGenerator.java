package org.fcrepo.server.management;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.fcrepo.common.MalformedPIDException;
import org.fcrepo.common.PID;
import org.fcrepo.server.Module;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.management.PIDGenerator;
import org.fcrepo.server.storage.highlevel.hadoop.HadoopHighLevelStorageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBasePIDGenerator extends Module implements PIDGenerator {
	private static final byte[] CURRENT_PID_KEY = "CURRENTPID".getBytes(HadoopHighLevelStorageProperties.getCharset());
	private static final byte[] CURRENT_PID_COL = "value".getBytes(HadoopHighLevelStorageProperties.getCharset());
	private static final byte[] RESERVED_PID_COL = "date_reserved".getBytes(HadoopHighLevelStorageProperties.getCharset());

	private static final Logger log = LoggerFactory.getLogger(HBasePIDGenerator.class);
	private final HadoopHighLevelStorageProperties properties;
	private final HTable pidTable;
	private final HTable reservedPIDsTable;

	public HBasePIDGenerator(Map<String, String> moduleParams, Server server, String role, HadoopHighLevelStorageProperties properties)
			throws ModuleInitializationException {
		super(moduleParams, server, role);
		this.properties = properties;
		try {
			pidTable = initTable(properties.getPidTableName(), Arrays.asList(new String(CURRENT_PID_COL,HadoopHighLevelStorageProperties.getCharset())));
			reservedPIDsTable = initTable(properties.getReservedPIDTableName(), Arrays.asList(new String(RESERVED_PID_COL,HadoopHighLevelStorageProperties.getCharset())));
		} catch (Exception e) {
			throw new ModuleInitializationException("unable to open PID generator tables in HBase", role, e);
		}
	}

	private HTable initTable(String name, List<String> columns) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(properties.getConfiguration());
		if (!admin.tableExists(name.getBytes(HadoopHighLevelStorageProperties.getCharset()))) {
			log.debug("creating table " + name);
			HTableDescriptor t = new HTableDescriptor(name.getBytes(HadoopHighLevelStorageProperties.getCharset()));
			for (String c : columns) {
				HColumnDescriptor cd = new HColumnDescriptor(c);
				t.addFamily(cd);
			}
			admin.createTable(t);
		}
		return new HTable(name);
	}

	@Override
	public PID generatePID(String namespace) throws IOException {
		PID last = getLastPID();
		long lastIndex=1;
		if (last!=null){
			String lastStr=last.toString();
			int pos = lastStr.indexOf(':');
			if (pos == -1) {
				throw new IOException("unable to generate PID. Last PID is not parseable");
			}
			lastIndex=Long.parseLong(lastStr.substring(pos + 1));
		}

		String next;
		int step = 1;
		do {
			next = namespace + ":" + (lastIndex + step);
		} while (isReserved(next));

		Put p = new Put(CURRENT_PID_KEY);
		long ts = System.currentTimeMillis();
		p.add(CURRENT_PID_COL, properties.getDefaultQualifierAsBytes(), ts, next.getBytes(HadoopHighLevelStorageProperties.getCharset()));
		pidTable.put(p);
		PID nextPid;
		try {
			nextPid = new PID(next);
			return nextPid;
		} catch (MalformedPIDException e) {
			throw new RuntimeException("unable to create new PID", e);
		}

	}

	private boolean isReserved(String pid) throws IOException {
		Get g = new Get(Bytes.toBytes(pid));
		if (reservedPIDsTable.get(g).isEmpty()) {
			return false;
		}
		return true;
	}

	@Override
	public PID getLastPID() throws IOException {
		Get g = new Get(CURRENT_PID_KEY);
		try {
			Result r=pidTable.get(g);
			if (r.isEmpty()){
				return null;
			}
			String lastPid = new String(r.getValue(CURRENT_PID_COL, properties.getDefaultQualifierAsBytes()),HadoopHighLevelStorageProperties.getCharset());
			PID last = new PID(lastPid);
			return last;
		} catch (MalformedPIDException e) {
			throw new RuntimeException("unable to fetch last PID from HBase", e);
		}
	}

	@Override
	public void neverGeneratePID(String pid) throws IOException {
		Put p = new Put(pid.getBytes(HadoopHighLevelStorageProperties.getCharset()));
		p.add(RESERVED_PID_COL,properties.getDefaultQualifierAsBytes(),System.currentTimeMillis(),Bytes.toBytes(System.currentTimeMillis()));
		reservedPIDsTable.put(p);
	}
}
