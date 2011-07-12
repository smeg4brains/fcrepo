package org.fcrepo.server.storage.highlevel.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.fcrepo.server.Module;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.LowlevelStorageException;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.storage.distributed.Atomic;
import org.fcrepo.server.storage.highlevel.HighlevelStorage;
import org.fcrepo.server.storage.highlevel.hadoop.HadoopHighLevelStorageProperties.Column;
import org.fcrepo.server.storage.translation.DOTranslationUtility;
import org.fcrepo.server.storage.translation.DOTranslator;
import org.fcrepo.server.storage.types.BasicDigitalObject;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DigitalObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopHighLevelStorage extends Module implements HighlevelStorage, Atomic {
	private static final Logger log = LoggerFactory.getLogger(HadoopHighLevelStorage.class);
	private static final Charset charset = Charset.defaultCharset();

	private final HTable objectTable;
	private final HTable datastreamTable;
	private final HadoopHighLevelStorageProperties props;

	private DOTranslator translator;

	public HadoopHighLevelStorage(Map<String, String> moduleParams, Server server, String role, HadoopHighLevelStorageProperties props)
			throws ModuleInitializationException {
		super(moduleParams, server, role);
		this.props = props;
		try {
			HBaseAdmin admin = new HBaseAdmin(props.getConfiguration());
			if (!admin.tableExists(props.getObjectTableNameAsBytes())) {
				log.debug("creating object table");
				HTableDescriptor ot = new HTableDescriptor(props.getObjectTableNameAsBytes());
				for (Column c:HadoopHighLevelStorageProperties.Column.values()){
					HColumnDescriptor colDesc = new HColumnDescriptor(c.toByteArray());
					ot.addFamily(colDesc);
				}
				admin.createTable(ot);
			}
			if (!admin.tableExists(props.getDatastreamTableNameAsBytes())) {
				log.debug("creating datastream table");
				HTableDescriptor dst = new HTableDescriptor(props.getDatastreamTableNameAsBytes());
				HColumnDescriptor contentCol = new HColumnDescriptor(HadoopHighLevelStorageProperties.Column.CONTENT_RAW.toByteArray());
				dst.addFamily(contentCol);
				admin.createTable(dst);
			}
			this.objectTable = new HTable(props.getObjectTableNameAsBytes());
			this.datastreamTable = new HTable(props.getDatastreamTableNameAsBytes());
		} catch (Exception e) {
			log.error(e.getLocalizedMessage(), e);
			throw new ModuleInitializationException("unable to initialize HBase connection: " + e.getLocalizedMessage(), role);
		}
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
			translator.serialize(object, out, props.getDefaultFormat(), props.getDefaultEncoding(), DOTranslationUtility.SERIALIZE_STORAGE_INTERNAL);
		} catch (ServerException e) {
			log.error("unable to serialize object", e);
			throw new LowlevelStorageException(false, e.getLocalizedMessage(), e);
		}
		Put p = new Put(key);
		long ts=System.currentTimeMillis();
		p.add(HadoopHighLevelStorageProperties.Column.C_DATE.toByteArray(),props.getDefaultQualifierAsBytes(),ts,Bytes.toBytes(object.getCreateDate().getTime()));
		p.add(HadoopHighLevelStorageProperties.Column.LABEL.toByteArray(),props.getDefaultQualifierAsBytes(),ts,Bytes.toBytes(object.getLabel()));
		p.add(HadoopHighLevelStorageProperties.Column.M_DATE.toByteArray(),props.getDefaultQualifierAsBytes(),ts,Bytes.toBytes(object.getLastModDate().getTime()));
		p.add(HadoopHighLevelStorageProperties.Column.OWNER_ID.toByteArray(),props.getDefaultQualifierAsBytes(),ts,Bytes.toBytes(object.getOwnerId()));
		p.add(HadoopHighLevelStorageProperties.Column.STATE.toByteArray(),props.getDefaultQualifierAsBytes(),ts,Bytes.toBytes(object.getState()));
		p.add(HadoopHighLevelStorageProperties.Column.CONTENT_RAW.toByteArray(), props.getDefaultQualifierAsBytes(), System.currentTimeMillis(), out.toByteArray());
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
					p.add(HadoopHighLevelStorageProperties.Column.CONTENT_RAW.toByteArray(), props.getDefaultQualifierAsBytes(), System.currentTimeMillis(), out.toByteArray());
					datastreamTable.put(p);
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
		return new ByteArrayInputStream(res.getValue(HadoopHighLevelStorageProperties.Column.CONTENT_RAW.toByteArray(), props.getDefaultQualifierAsBytes()));
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
			translator.deserialize(new ByteArrayInputStream(res.getValue(HadoopHighLevelStorageProperties.Column.CONTENT_RAW.toByteArray(), props.getDefaultQualifierAsBytes())), obj,
					props.getDefaultFormat(), props.getDefaultEncoding(), DOTranslationUtility.SERIALIZE_STORAGE_INTERNAL);
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
			translator.serialize(newVersion, out, props.getDefaultFormat(), props.getDefaultEncoding(), DOTranslationUtility.SERIALIZE_STORAGE_INTERNAL);
		} catch (ServerException e) {
			log.error("Unable to update object " + oldVersion.getPid() + " with " + newVersion.getPid());
			throw new LowlevelStorageException(false, e.getLocalizedMessage(), e);
		}
		long ts=System.currentTimeMillis();
		p.add(HadoopHighLevelStorageProperties.Column.CONTENT_RAW.toByteArray(), props.getDefaultQualifierAsBytes(), ts, out.toByteArray());
		p.add(HadoopHighLevelStorageProperties.Column.C_DATE.toByteArray(),props.getDefaultQualifierAsBytes(),ts,Bytes.toBytes(newVersion.getCreateDate().getTime()));
		p.add(HadoopHighLevelStorageProperties.Column.LABEL.toByteArray(),props.getDefaultQualifierAsBytes(),ts,Bytes.toBytes(newVersion.getLabel()));
		p.add(HadoopHighLevelStorageProperties.Column.M_DATE.toByteArray(),props.getDefaultQualifierAsBytes(),ts,Bytes.toBytes(newVersion.getLastModDate().getTime()));
		p.add(HadoopHighLevelStorageProperties.Column.OWNER_ID.toByteArray(),props.getDefaultQualifierAsBytes(),ts,Bytes.toBytes(newVersion.getOwnerId()));
		p.add(HadoopHighLevelStorageProperties.Column.STATE.toByteArray(),props.getDefaultQualifierAsBytes(),ts,Bytes.toBytes(newVersion.getState()));
		p.add(HadoopHighLevelStorageProperties.Column.CONTENT_RAW.toByteArray(), props.getDefaultQualifierAsBytes(), ts, out.toByteArray());
		try{
			objectTable.put(p);
		}catch(Exception e){
			log.error("Unable to update object",e);
			throw new LowlevelStorageException(false, "unable to update object",e);
		}
		List<String> oldIds=new ArrayList<String>();
		Iterator<String> oldDatastreams = oldVersion.datastreamIdIterator();
		while (oldDatastreams.hasNext()){
			oldIds.add(oldDatastreams.next());
		}
		Iterator<String> datastreams = newVersion.datastreamIdIterator();
		while (datastreams.hasNext()) {
			String id = datastreams.next();
			Put dsPut = new Put(id.getBytes(charset));
			for (Datastream ds : newVersion.datastreams(id)) {
				InputStream in = null;
				try {
					out = new ByteArrayOutputStream(1024);
					in = ds.getContentStream();
					IOUtils.copy(in, out);
					dsPut.add(HadoopHighLevelStorageProperties.Column.CONTENT_RAW.toByteArray(), props.getDefaultQualifierAsBytes(), System.currentTimeMillis(), out.toByteArray());
					datastreamTable.put(dsPut);
				} catch (Exception e) {
					throw new LowlevelStorageException(false, e.getLocalizedMessage(), e);
				} finally {
					IOUtils.closeQuietly(in);
					IOUtils.closeQuietly(out);
				}
			}

		}
	}
}
