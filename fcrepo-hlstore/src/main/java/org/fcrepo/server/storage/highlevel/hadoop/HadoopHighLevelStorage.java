package org.fcrepo.server.storage.highlevel.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.fcrepo.common.Constants;
import org.fcrepo.common.Models;
import org.fcrepo.server.Module;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.LowlevelStorageException;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.storage.distributed.Atomic;
import org.fcrepo.server.storage.distributed.DeploymentManager;
import org.fcrepo.server.storage.highlevel.HighlevelStorage;
import org.fcrepo.server.storage.translation.DOTranslationUtility;
import org.fcrepo.server.storage.translation.DOTranslator;
import org.fcrepo.server.storage.types.BasicDigitalObject;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DigitalObject;
import org.fcrepo.server.storage.types.RelationshipTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopHighLevelStorage extends Module implements HighlevelStorage, Atomic {
	private static final Logger log = LoggerFactory.getLogger(HadoopHighLevelStorage.class);
	private static final Charset charset = Charset.defaultCharset();
	public static final String OBJECT_TABLE_COL_LABEL = "label";
	public static final String OBJECT_TABLE_COL_OWNER = "owner_id";
	public static final String OBJECT_TABLE_COL_CREATION_DATE = "c_date";
	public static final String OBJECT_TABLE_COL_MODIFICATION_DATE = "m_date";
	public static final String OBJECT_TABLE_COL_STATE = "state";
	public static final String OBJECT_TABLE_COL_CONTENT = "content_raw";
	public static final String DATASTREAM_TABLE_COL_CONTENT = "content_raw";

	private final HTable objectTable;
	private final HTable datastreamTable;
	private final HadoopProperties props;
	private DeploymentManager deploymentManager;

	private DOTranslator translator;

	public HadoopHighLevelStorage(Map<String, String> moduleParams, Server server, String role, HadoopProperties props, HBaseAdminTool adminTool)
			throws ModuleInitializationException {
		super(moduleParams, server, role);
		this.props = props;
		try {
			if (adminTool.tableExists(props.getObjectTableName())) {
				objectTable = new HTable(Bytes.toBytes(props.getObjectTableName()));
			} else {
				objectTable = adminTool.createTable(props.getObjectTableName(), Arrays.asList(OBJECT_TABLE_COL_LABEL, OBJECT_TABLE_COL_OWNER,
						OBJECT_TABLE_COL_STATE, OBJECT_TABLE_COL_CREATION_DATE, OBJECT_TABLE_COL_MODIFICATION_DATE, OBJECT_TABLE_COL_CONTENT));
			}
			if (adminTool.tableExists(props.getDatastreamTableName())) {
				datastreamTable = new HTable(Bytes.toBytes(props.getDatastreamTableName()));
			} else {
				datastreamTable = adminTool.createTable(props.getDatastreamTableName(), Arrays.asList(DATASTREAM_TABLE_COL_CONTENT));
			}
		} catch (Exception e) {
			log.error(e.getLocalizedMessage(), e);
			throw new ModuleInitializationException("unable to initialize HBase connection: " + e.getLocalizedMessage(), role);
		}
	}

	public void setDeploymentManager(DeploymentManager deploymentManager) {
		this.deploymentManager = deploymentManager;
	}

	public void setTranslator(DOTranslator translator) {
		this.translator = translator;
	}

	@Override
	public void add(DigitalObject object) throws LowlevelStorageException {
		byte[] key = object.getPid().getBytes(charset);
		addDatastreams(object);
		ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
		log.debug("saving object with pid " + object.getPid());
		log.debug("label: " + object.getLabel());
		try {
			translator.serialize(object, out, Bytes.toString(props.getFormat()), Bytes.toString(props.getEncoding()),
					DOTranslationUtility.SERIALIZE_STORAGE_INTERNAL);
		} catch (ServerException e) {
			log.error("unable to serialize object", e);
			throw new LowlevelStorageException(false, e.getLocalizedMessage(), e);
		}
		Put p = new Put(key);
		long ts = System.currentTimeMillis();
		p.add(Bytes.toBytes(OBJECT_TABLE_COL_CREATION_DATE), props.getQualifier(), ts, Bytes.toBytes(object.getCreateDate().getTime()));
		p.add(Bytes.toBytes(OBJECT_TABLE_COL_LABEL), props.getQualifier(), ts, Bytes.toBytes(object.getLabel()));
		p.add(Bytes.toBytes(OBJECT_TABLE_COL_MODIFICATION_DATE), props.getQualifier(), ts, Bytes.toBytes(object.getLastModDate().getTime()));
		p.add(Bytes.toBytes(OBJECT_TABLE_COL_OWNER), props.getQualifier(), ts, Bytes.toBytes(object.getOwnerId()));
		p.add(Bytes.toBytes(OBJECT_TABLE_COL_STATE), props.getQualifier(), ts, Bytes.toBytes(object.getState()));
		p.add(Bytes.toBytes(OBJECT_TABLE_COL_CONTENT), props.getQualifier(), System.currentTimeMillis(), out.toByteArray());
		log.debug("extended properties:");
		for (Entry<String, String> entry : object.getExtProperties().entrySet()) {
			log.debug("property: " + entry.getKey() + " value: " + entry.getValue());
		}
		try {
			this.objectTable.put(p);
		} catch (IOException e) {
			log.error("unable to save object to HBase", e);
			throw new LowlevelStorageException(false, e.getLocalizedMessage(), e);
		}
	}

	private void addDatastreams(DigitalObject object) throws LowlevelStorageException {
		Iterator<String> dsIds = object.datastreamIdIterator();
		while (dsIds.hasNext()) {
			String dsId = dsIds.next();
			for (Datastream ds : object.datastreams(dsId)) {
				Put p = new Put(dsId.getBytes(charset));
				ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
				InputStream in = null;
				try {
					in = ds.getContentStream();
					IOUtils.copy(in, out);
					p.add(Bytes.toBytes(DATASTREAM_TABLE_COL_CONTENT), props.getQualifier(), System.currentTimeMillis(), out.toByteArray());
					datastreamTable.put(p);
					if (object.hasContentModel(Models.SERVICE_DEPLOYMENT_3_0)) {
						String sDep = object.getPid();
						Set<RelationshipTuple> sDefs = object.getRelationships(Constants.MODEL.IS_DEPLOYMENT_OF, null);
						Set<RelationshipTuple> models = object.getRelationships(Constants.MODEL.IS_CONTRACTOR_OF, null);
						for (RelationshipTuple sDef : sDefs) {
							for (RelationshipTuple model : models) {
								deploymentManager.addDeployment(model.getObjectPID(), sDef.getObjectPID(), object.getPid());
							}
						}
					}
					// add service deployments
				} catch (Exception e) {
					log.error("Unable to save datastream " + dsId);
					throw new LowlevelStorageException(false, e.getLocalizedMessage(), e);
				} finally {
					IOUtils.closeQuietly(in);
					IOUtils.closeQuietly(out);
				}
			}
		}
	}

	@Override
	public boolean exists(String pid) throws LowlevelStorageException {
		Get g = new Get(pid.getBytes(charset));
		try {
			if (!objectTable.get(g).isEmpty()) {
				return true;
			}
		} catch (IOException e) {
			log.error("unable to chekc for existance in HBase", e);
			throw new LowlevelStorageException(false, e.getLocalizedMessage(), e);
		}
		return false;
	}

	@Override
	public void purge(String pid) throws LowlevelStorageException {
		DigitalObject obj = readObject(pid);
		purgeDatastreams(obj.datastreamIdIterator());
		Delete d = new Delete(pid.getBytes(charset));
		try {
			objectTable.delete(d);
		} catch (IOException e) {
			log.debug("Unable to delete object with pid " + pid);
			throw new LowlevelStorageException(false, e.getLocalizedMessage(), e);
		}

	}

	private void purgeDatastreams(Iterator<String> datastreamIdIterator) throws LowlevelStorageException {
		while (datastreamIdIterator.hasNext()) {
			String id = datastreamIdIterator.next();
			Delete d = new Delete(id.getBytes(charset));
			try {
				datastreamTable.delete(d);
			} catch (IOException e) {
				log.error("unable to purge datastream " + id);
				throw new LowlevelStorageException(false, "unable to delete datastream", e);
			}
		}
	}

	@Override
	public InputStream readDatastream(DigitalObject object, Datastream datastream) throws LowlevelStorageException {
		Get g = new Get(datastream.DatastreamID.getBytes(charset));
		Result res;
		try {
			res = datastreamTable.get(g);
		} catch (IOException e) {
			log.error("unable to fetch datastream from table", e);
			throw new LowlevelStorageException(false, e.getLocalizedMessage(), e);
		}
		if (res.isEmpty()) {
			throw new LowlevelStorageException(false, "Unable to find datastream " + datastream.DatastreamID);
		}
		return new ByteArrayInputStream(res.getValue(Bytes.toBytes(DATASTREAM_TABLE_COL_CONTENT), props.getQualifier()));
	}

	@Override
	public DigitalObject readObject(String pid) throws LowlevelStorageException {
		Get g = new Get(pid.getBytes(charset));
		Result res;
		try {
			res = objectTable.get(g);
		} catch (IOException e) {
			log.error("unable to get object from HBase with pid " + pid);
			throw new LowlevelStorageException(false, e.getLocalizedMessage(), e);
		}
		if (res.isEmpty()) {
			throw new LowlevelStorageException(false, "Unable to find object with pid " + pid);
		}
		DigitalObject obj = new BasicDigitalObject();
		try {
			translator.deserialize(new ByteArrayInputStream(res.getValue(Bytes.toBytes(OBJECT_TABLE_COL_CONTENT), props.getQualifier())), obj,
					Bytes.toString(props.getFormat()), Bytes.toString(props.getEncoding()), DOTranslationUtility.SERIALIZE_STORAGE_INTERNAL);
		} catch (Exception e) {
			log.error("unable to get deserialize object from HBase with pid " + pid);
			throw new LowlevelStorageException(false, e.getLocalizedMessage(), e);
		}
		return obj;
	}

	@Override
	public void update(DigitalObject oldVersion, DigitalObject newVersion) throws LowlevelStorageException {
		log.debug("updating digital object " + oldVersion.getPid());
		Put p = new Put(newVersion.getPid().getBytes(charset));
		ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
		try {
			translator.serialize(newVersion, out, Bytes.toString(props.getFormat()), Bytes.toString(props.getEncoding()),
					DOTranslationUtility.SERIALIZE_STORAGE_INTERNAL);
		} catch (ServerException e) {
			log.error("Unable to update object " + oldVersion.getPid() + " with " + newVersion.getPid());
			throw new LowlevelStorageException(false, e.getLocalizedMessage(), e);
		}
		long ts = System.currentTimeMillis();
		p.add(Bytes.toBytes(OBJECT_TABLE_COL_CONTENT), props.getQualifier(), ts, out.toByteArray());
		p.add(Bytes.toBytes(OBJECT_TABLE_COL_CREATION_DATE), props.getQualifier(), ts, Bytes.toBytes(newVersion.getCreateDate().getTime()));
		p.add(Bytes.toBytes(OBJECT_TABLE_COL_LABEL), props.getQualifier(), ts, Bytes.toBytes(newVersion.getLabel()));
		p.add(Bytes.toBytes(OBJECT_TABLE_COL_MODIFICATION_DATE), props.getQualifier(), ts, Bytes.toBytes(newVersion.getLastModDate().getTime()));
		p.add(Bytes.toBytes(OBJECT_TABLE_COL_OWNER), props.getQualifier(), ts, Bytes.toBytes(newVersion.getOwnerId()));
		p.add(Bytes.toBytes(OBJECT_TABLE_COL_STATE), props.getQualifier(), ts, Bytes.toBytes(newVersion.getState()));
		try {
			objectTable.put(p);
		} catch (Exception e) {
			log.error("Unable to update object", e);
			throw new LowlevelStorageException(false, "unable to update object", e);
		}
		List<String> oldIds = new ArrayList<String>();
		Iterator<String> oldDatastreams = oldVersion.datastreamIdIterator();
		while (oldDatastreams.hasNext()) {
			oldIds.add(oldDatastreams.next());
		}
		Iterator<String> datastreams = newVersion.datastreamIdIterator();
		while (datastreams.hasNext()) {
			String id = datastreams.next();
			oldIds.remove(id);
			Put dsPut = new Put(id.getBytes(charset));
			for (Datastream ds : newVersion.datastreams(id)) {
				InputStream in = null;
				try {
					out = new ByteArrayOutputStream(1024);
					in = ds.getContentStream();
					IOUtils.copy(in, out);
					dsPut.add(Bytes.toBytes(OBJECT_TABLE_COL_CONTENT), props.getQualifier(), System.currentTimeMillis(), out.toByteArray());
					datastreamTable.put(dsPut);
				} catch (Exception e) {
					throw new LowlevelStorageException(false, e.getLocalizedMessage(), e);
				} finally {
					IOUtils.closeQuietly(in);
					IOUtils.closeQuietly(out);
				}
			}
		}
		for (String oldId : oldIds) {
			Delete d = new Delete(Bytes.toBytes(oldId));
			try {
				datastreamTable.delete(d);
			} catch (IOException e) {
				throw new LowlevelStorageException(false, "unable to delete unreferenced datastream " + oldId, e);
			}
		}
	}

	@Override
	public InputStream readDatastream(String datastreamId) {
		Get g = new Get(Bytes.toBytes(datastreamId));
		Result res;
		try {
			res = datastreamTable.get(g);
		} catch (IOException e) {
			throw new RuntimeException(e.getLocalizedMessage(), e);
		}
		ByteArrayInputStream stream=new ByteArrayInputStream(res.getValue(Bytes.toBytes(DATASTREAM_TABLE_COL_CONTENT), props.getQualifier()));
		return stream;
	}
}
