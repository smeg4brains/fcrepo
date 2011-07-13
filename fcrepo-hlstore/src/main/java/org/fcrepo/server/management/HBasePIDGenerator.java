package org.fcrepo.server.management;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.fcrepo.common.MalformedPIDException;
import org.fcrepo.common.PID;
import org.fcrepo.server.Module;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.storage.highlevel.hadoop.HBaseAdminTool;
import org.fcrepo.server.storage.highlevel.hadoop.HadoopProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBasePIDGenerator extends Module implements PIDGenerator {
	private static final Logger log = LoggerFactory.getLogger(HBasePIDGenerator.class);
	private static final String PID_TABLE_COL_VALUE="value";
	private static final String PID_TABLE_KEY="current_pid";
	private static final String RESERVED_PID_TABLE_COL_DATE="date"; 

	private final HadoopProperties properties;
	private final HTable pidTable;
	private final HTable reservedPIDsTable;
	private final HBaseAdminTool adminTool;

	public HBasePIDGenerator(Map<String, String> moduleParams, Server server, String role, HadoopProperties properties,HBaseAdminTool adminTool)
			throws ModuleInitializationException {
		super(moduleParams, server, role);
		this.properties = properties;
		this.adminTool=adminTool;
		try {
			if (this.adminTool.tableExists(properties.getPidTableName())){
				pidTable=new HTable(Bytes.toBytes(properties.getPidTableName()));
			}else{
				pidTable=this.adminTool.createTable(properties.getPidTableName(), Arrays.asList(PID_TABLE_COL_VALUE));
			}
			if (this.adminTool.tableExists(properties.getReservedPidsTableName())){
				reservedPIDsTable=new HTable(Bytes.toBytes(properties.getReservedPidsTableName()));
			}else{
				reservedPIDsTable=adminTool.createTable(properties.getReservedPidsTableName(), Arrays.asList(RESERVED_PID_TABLE_COL_DATE));
			}
		} catch (Exception e) {
			throw new ModuleInitializationException("unable to open PID generator tables in HBase", role, e);
		}
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

		Put p = new Put(Bytes.toBytes(PID_TABLE_KEY));
		long ts = System.currentTimeMillis();
		p.add(Bytes.toBytes(PID_TABLE_COL_VALUE), properties.getQualifier(), ts, Bytes.toBytes(next));
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
		Get g = new Get(Bytes.toBytes(PID_TABLE_KEY));
		try {
			Result r=pidTable.get(g);
			if (r.isEmpty()){
				return null;
			}
			String lastPid = Bytes.toString(r.getValue(Bytes.toBytes(PID_TABLE_COL_VALUE), properties.getQualifier()));
			PID last = new PID(lastPid);
			return last;
		} catch (MalformedPIDException e) {
			throw new RuntimeException("unable to fetch last PID from HBase", e);
		}
	}

	@Override
	public void neverGeneratePID(String pid) throws IOException {
		Put p = new Put(Bytes.toBytes(pid));
		p.add(Bytes.toBytes(RESERVED_PID_TABLE_COL_DATE),properties.getQualifier(),System.currentTimeMillis(),Bytes.toBytes(System.currentTimeMillis()));
		reservedPIDsTable.put(p);
	}
}
