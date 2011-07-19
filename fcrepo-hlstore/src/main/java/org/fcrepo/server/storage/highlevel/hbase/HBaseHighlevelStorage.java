
package org.fcrepo.server.storage.highlevel.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.fcrepo.server.management.Management;
import org.fcrepo.server.storage.ExternalContentManager;
import org.fcrepo.server.storage.distributed.Atomic;
import org.fcrepo.server.storage.highlevel.HighlevelDigitalObject;
import org.fcrepo.server.storage.highlevel.HighlevelStorage;
import org.fcrepo.server.storage.highlevel.ManagedDatastreamComparator;

import org.fcrepo.common.Constants;

import org.fcrepo.server.Module;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.LowlevelStorageException;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.errors.ObjectNotInLowlevelStorageException;
import org.fcrepo.server.errors.StreamIOException;
import org.fcrepo.server.storage.translation.DOTranslationUtility;
import org.fcrepo.server.storage.translation.DOTranslator;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DigitalObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/*
 * TODO: be aware of versioning: throw exception if table doesn't accommodate
 * specified number of versions, skip cleanup if we know older datastream
 * versions will be purged
 */
public class HBaseHighlevelStorage
        extends Module
        implements HighlevelStorage, Atomic {

    private DOTranslator m_translator;

    private String m_encoding = DEFAULT_ENCODING;

    private String m_format = DEFAULT_FORMAT;

    private String m_root;

    private String m_master;

    private Management m_mgmt;

    private ExternalContentManager m_ecm;

    private static final Logger LOG = LoggerFactory
            .getLogger(HBaseHighlevelStorage.class);

    private final ThreadLocal<HTable> m_table = new ThreadLocal<HTable>() {

        @Override
        protected HTable initialValue() {
            return getHTable();
        }
    };

    /**
     * Location of HBase files for standalone mode.
     * <p>
     * Used with standalone filesystem Base only (testing & experimenting).
     * </p>
     *
     * @param root
     *        File URI pointing to HBase data root directory.
     */
    public void setHBaseRoot(String root) {
        m_root = root;
    }

    /**
     * Address of the HBase master node.
     * <p>
     * Used with distributed HBbase only.
     * </p>
     *
     * @param master
     *        Host and port of the HBase master node.
     */
    public void setHBaseMaster(String master) {
        m_master = master;
    }

    public void setHBaseTable(String table) {
        m_tableName = table;
    }

    /**
     * Set fedora object serialization format.
     *
     * @param format
     *        Format URI. Default is {@value #DEFAULT_FORMAT}.
     */
    public void setFormat(String format) {
        m_format = format;
    }

    /**
     * Set Character encoding for fedora object serialization
     *
     * @param encoding
     *        Encoding. Default is {@value #DEFAULT_ENCODING}.
     */
    public void setEncoding(String encoding) {
        m_encoding = encoding;
    }

    @Required
    public void setDOTranslator(DOTranslator dot) {
        m_translator = dot;
    }

    @Required
    public void setManagement(Management mgmt) {
        m_mgmt = mgmt;
    }

    @Required
    public void setExternalContentManager(ExternalContentManager ecm) {
        m_ecm = ecm;
    }

    private static final String PROP_HBASE_ROOTDIR = "root";

    private static final String PROP_HBASE_MASTER = "master";

    private static final String PROP_FORMAT = "format";

    private static final String DEFAULT_FORMAT = Constants.FOXML1_1.toString();

    private static final String PROP_ENCODING = "encoding";

    private static final String DEFAULT_ENCODING = "UTF-8";

    private static final String PROP_TABLE = "table";

    private static final byte[] NULL_BYTES = new byte[0];

    private Configuration m_hbase_conf;

    private String m_tableName;

    public HBaseHighlevelStorage(Map<String, String> moduleParameters,
                                 Server server,
                                 String role)
            throws ModuleInitializationException {
        super(moduleParameters, server, role);

        m_root = getParameter(PROP_HBASE_ROOTDIR);

        m_master = getParameter(PROP_HBASE_MASTER);

        m_tableName = getParameter(PROP_TABLE);

        if ((m_format = getParameter(PROP_FORMAT)) == null) {
            m_format = DEFAULT_FORMAT;
        }

        if ((m_encoding = getParameter(PROP_ENCODING)) == null) {
            m_encoding = DEFAULT_ENCODING;
        }
    }

    @Override
    @PostConstruct
    public void initModule() throws ModuleInitializationException {

        m_hbase_conf = HBaseConfiguration.create();
        if (m_master != null) {
            m_hbase_conf.set(PROP_HBASE_MASTER, m_master);
        } else if (m_root != null) {
            m_hbase_conf.set(PROP_HBASE_ROOTDIR, m_root);
        } else {
            throw new ModuleInitializationException("HBase root or master must be set!",
                                                    HighlevelStorage.class
                                                            .getName());
        }
        m_hbase_conf.set(PROP_HBASE_ROOTDIR, m_root);

        /* verify table exists, and try instantiating one */
        try {
            HBaseAdmin hbase = new HBaseAdmin(m_hbase_conf);
            if (!hbase.tableExists(m_tableName)) {
                LOG.info("No HBase table " + m_tableName + ", creating");
                initTable();
            } else {
                LOG.info("Table " + m_tableName + "is enabled");
            }
        } catch (IOException e) {
            throw new ModuleInitializationException("Could not contact HBase",
                                                    getRole(),
                                                    e);
        }
        m_table.set(getHTable());

    }

    public void add(DigitalObject obj) throws LowlevelStorageException {

        LOG.info("add " + obj.getPid());

        /* Get newest timestamp. Last known is 0 */
        byte[] old_ts = NULL_BYTES;
        byte[] new_ts = getBytes(obj.getLastModDate().getTime());

        /* Use the PID as the row name */
        String pid = obj.getPid();
        byte[] row = pid.getBytes();
        Put append = new Put(row);

        /* Add foxml */
        append.add(Family.OBJECT, Column.CONTENT, serialize(obj));

        /* Mark format and encoding */
        append.add(Family.OBJECT, Column.FORMAT, m_format.getBytes());
        append.add(Family.OBJECT, Column.ENCODING, m_encoding.getBytes());

        /*
         * Add datastreams. Use their declared creation date as the cell
         * timestamp
         */
        ManagedDatastreamComparator managedStreams =
                new ManagedDatastreamComparator(obj);

        for (Datastream ds : managedStreams.getAdded()) {
            try {
                append.add(Family.DATASTREAM,
                           ds.DatastreamID.getBytes(),
                           getDStime(ds),
                           getBytes(ds.getContentStream()));
            } catch (StreamIOException e) {
                throw new LowlevelStorageException(true,
                                                   "Error reading datastream content of "
                                                           + ds.DatastreamID
                                                           + " for object "
                                                           + pid);
            }
        }

        SafeWrite write = new SafeWrite(pid, old_ts, new_ts, m_table.get());
        write.addStep(append);
        write.execute();
        LOG.info("Added " + obj.getPid());
        showDatastreams(pid, "afterAdd");
    }

    public void update(DigitalObject oldVersion, DigitalObject newVersion)
            throws LowlevelStorageException {

        LOG.info("update: " + oldVersion.getPid().toString());
        showDatastreams(newVersion.getPid(), "beforeUpdate");
        /* Get timestamps */
        byte[] new_ts = getBytes(newVersion.getLastModDate().getTime());
        byte[] old_ts = getBytes(oldVersion.getLastModDate().getTime());

        /* Use the PID as the row name */
        String pid = newVersion.getPid();
        byte[] row = pid.getBytes();

        SafeWrite write = new SafeWrite(pid, old_ts, new_ts, m_table.get());
        Put addsAndClears = new Put(row);
        Delete dropDSVersions = new Delete(row);

        addsAndClears.add(Family.OBJECT, Column.CONTENT, serialize(newVersion));

        /* Mark format and encoding */
        addsAndClears.add(Family.OBJECT, Column.FORMAT, m_format.getBytes());
        addsAndClears
                .add(Family.OBJECT, Column.ENCODING, m_encoding.getBytes());

        ManagedDatastreamComparator managedStreams =
                new ManagedDatastreamComparator(oldVersion, newVersion);

        for (Datastream d : managedStreams.getAdded()) {

            LOG.info("ADDING managed datastream " + d.DatastreamID + " dsTime:"
                    + getDStime(d) + " createDT: " + d.DSCreateDT.getTime()
                    + " DSLocation: " + d.DSLocation);
            try {
                addsAndClears.add(Family.DATASTREAM,
                                  d.DatastreamID.getBytes(),
                                  getDStime(d),
                                  getBytes(d.getContentStream()));
            } catch (StreamIOException e) {
                throw new LowlevelStorageException(true,
                                                   "Error reading datastream content of "
                                                           + d.DatastreamID
                                                           + " for object "
                                                           + pid,
                                                   e);
            }
        }
        for (Datastream d : managedStreams.getDeleted()) {
            LOG.info("DELETING managed datastream " + d.DatastreamID
                    + " dsTime:" + getDStime(d) + " createDT: "
                    + d.DSCreateDT.getTime() + " DSLocation: " + d.DSLocation);
            dropDSVersions.deleteColumn(Family.DATASTREAM,
                                        d.DatastreamID.getBytes(),
                                        getDStime(d));
            addsAndClears.add(Family.DATASTREAM,
                              d.DatastreamID.getBytes(),
                              getDStime(d),
                              NULL_BYTES);
        }

        write.addStep(addsAndClears);

        if (managedStreams.getAdded().isEmpty()) {
            LOG.info("NO managed datastreams to add");
        }

        if (!managedStreams.getDeleted().isEmpty()) {
            write.addStep(dropDSVersions);
        }
        write.execute();
        showDatastreams(newVersion.getPid(), "afterUpdate");
    }

    public DigitalObject readObject(String objectKey)
            throws LowlevelStorageException {
        LOG.info("readObject: " + objectKey);
        Get obj = new Get(objectKey.getBytes());
        obj.addColumn(Family.OBJECT, Column.CONTENT);
        obj.addColumn(Family.OBJECT, Column.FORMAT);
        obj.addColumn(Family.OBJECT, Column.ENCODING);

        try {
            Result row = m_table.get().get(obj);

            if (row.isEmpty()) {
                throw new ObjectNotInLowlevelStorageException(objectKey + " not found");
            }

            /* If the format and encoding aren't in the table, use the defaults */
            byte[] encoding = row.getValue(Family.OBJECT, Column.ENCODING);
            byte[] format = row.getValue(Family.OBJECT, Column.FORMAT);

            showDatastreams(objectKey, "readObject");

            return fixDSLocations(deserialize(row.getValue(Family.OBJECT,
                                                           Column.CONTENT),
                                              format != null ? new String(format)
                                                      : m_format,
                                              encoding != null ? new String(encoding)
                                                      : m_encoding));
        } catch (IOException e) {
            throw new LowlevelStorageException(false,
                                               "Could not retrieve object",
                                               e);
        }
    }

    public InputStream readDatastream(DigitalObject object,
                                      Datastream datastream)
            throws LowlevelStorageException {

        LOG.info("readDatastream: Interested in DS: " + datastream.DSLocation);
        showDatastreams(object.getPid(), "readDatastream");

        try {
            try {
                byte[] dsID = datastream.DatastreamID.getBytes();

                Get ds = new Get(object.getPid().getBytes());
                ds.addColumn(Family.DATASTREAM, dsID);

                long dsTime = getDStime(datastream);

                if (datastream.DSLocation.contains("copy://")) {
                    dsTime =
                            Long.parseLong(datastream.DSLocation.split("\\+")[2]);
                }

                LOG.info("readDatastream: reading datastream with TS " + dsTime);
                ds.setTimeStamp(dsTime);

                return new ByteArrayInputStream(m_table.get().get(ds)
                        .getValue(Family.DATASTREAM, dsID));
            } catch (NullPointerException e) {

                throw new LowlevelStorageException(true,
                                                   "Could not retrieve datastream with timestamp "
                                                           + getDStime(datastream),
                                                   e);
            }
        } catch (IOException e) {
            throw new LowlevelStorageException(false,
                                               "Could not retrieve datastream",
                                               e);
        }
    }

    public boolean exists(String pid) throws LowlevelStorageException {
        LOG.info("exists: " + pid);
        try {
            return m_table.get().exists(new Get(pid.getBytes()));
        } catch (IOException e) {
            throw new LowlevelStorageException(true, "Errorfinding object "
                    + pid, e);
        }
    }

    public void purge(String pid) throws LowlevelStorageException {
        LOG.info("purge: " + pid);
        try {
            m_table.get().delete(new Delete(pid.getBytes()));
        } catch (IOException e) {
            throw new LowlevelStorageException(true, "Error purging object "
                    + pid, e);
        }
    }

    private HTable getHTable() {
        if (m_hbase_conf != null) {
            try {
                LOG.info("Loading table '" + m_tableName + "'");
                return new HTable(m_hbase_conf, m_tableName);
            } catch (IOException e) {
                throw new RuntimeException("Error connecting to HTable", e);
            }
        } else {
            return null;
        }
    }

    private byte[] getBytes(InputStream stream) {

        try {

            if (stream instanceof FileInputStream) {
                return getBytesFromFile((FileInputStream) stream);
            }

            ByteArrayOutputStream o = new ByteArrayOutputStream();
            byte[] b = new byte[4096];
            for (int n; (n = stream.read(b)) != -1;) {
                o.write(b, 0, n);
            }
            return o.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Error processing stream", e);
        }
    }

    /* Read bytes directly into a properly sized buffer */
    private byte[] getBytesFromFile(FileInputStream file) throws IOException {
        FileChannel fc = file.getChannel();
        final int SIZE = (int) fc.size();

        if (SIZE > Integer.MAX_VALUE) {
            throw new RuntimeException("File is too big!  Absolute maximum is "
                    + Integer.MAX_VALUE + " bytes, data is " + SIZE);
        }

        ByteBuffer bytes = ByteBuffer.allocate(SIZE);

        for (int total = 0; total < SIZE; total += fc.read(bytes));

        return bytes.array();
    }

    private byte[] serialize(DigitalObject obj) throws LowlevelStorageException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            m_translator
                    .serialize(obj,
                               out,
                               m_format,
                               m_encoding,
                               DOTranslationUtility.SERIALIZE_STORAGE_INTERNAL);
        } catch (Exception e) {
            throw new LowlevelStorageException(true,
                                               "Could not serialize object",
                                               e);
        }
        return out.toByteArray();
    }

    private DigitalObject deserialize(byte[] foxmlContent,
                                      String format,
                                      String encoding)
            throws LowlevelStorageException {
        DigitalObject obj = new HighlevelDigitalObject(this, m_mgmt, m_ecm);

        try {
            m_translator.deserialize(new ByteArrayInputStream(foxmlContent),
                                     obj,
                                     format,
                                     encoding,
                                     DOTranslationUtility.DESERIALIZE_INSTANCE);
        } catch (Exception e) {
            throw new LowlevelStorageException(true,
                                               "Could not deserialize object",
                                               e);
        }
        return obj;
    }

    private byte[] getBytes(long l) {
        return Long.toHexString(l).getBytes();
    }

    /*
     * DoSerializers don't respect dsLocation of datastreams - they always
     * replace the value with PID + DSID + versionID. This is not useful for
     * HBase, since datastream retrieval is date oriented. To get around this
     * serializer defect, we have to fix the DSLocation values after
     * deserializing, every time we deserialize
     */
    private DigitalObject fixDSLocations(DigitalObject obj) {
        Iterator<String> DSIDs = obj.datastreamIdIterator();

        while (DSIDs.hasNext()) {
            for (Datastream d : obj.datastreams(DSIDs.next())) {
                if (d.DSControlGrp.equals("M") && !d.DSLocation.contains(":/")) {
                    setDSLocation(obj.getPid(), d);
                    LOG.info("Set DSLocation to " + d.DSLocation);
                } else {
                    LOG.info("Not changing dsLocation for " + d.DatastreamID
                            + ", (" + d.DSControlGrp + "), (" + d.DSLocation
                            + ")");
                }
            }
        }

        DSIDs = obj.datastreamIdIterator();
        while (DSIDs.hasNext()) {
            for (Datastream d : obj.datastreams(DSIDs.next())) {
                LOG.info("DSLocation " + d.DatastreamID + " (" + d.DSControlGrp
                        + ") : " + d.DSLocation);
            }
        }

        return obj;
    }

    /*
     * Sets the DSLocation in a datastream to something useful for
     * HBaseHighlevelStorage (i.e. contains ds time)
     */
    private void setDSLocation(String pid, Datastream d) {
        d.DSLocation =
                pid + "+" + d.DatastreamID + "+"
                        + Long.toString(d.DSCreateDT.getTime());
    }

    /*
     * Since datastreams are stored using their creation time as the cell time
     * in HBase, this gets the proper time value. It's encoded in DSLocation
     */
    private long getDStime(Datastream d) {
        try {
            if (!d.DSLocation.contains(":/")) {
                long locationDate =
                        Long.parseLong(d.DSLocation.split("\\+")[2]);
                long streamDate = d.DSCreateDT.getTime();

                if (streamDate > locationDate) {
                    LOG.info("getDSTime: DSCreateDT > DSLocation, so using DSCreateDT ("
                            + streamDate + ")");
                    return streamDate;
                } else {
                    LOG.info("getDSTime: DSCreateDt <= DSLocation, so using DSLocation ("
                            + locationDate + ")");
                    return locationDate;
                }
            }
        } catch (Exception e) {
        }
        LOG.info("Could not read DSLocation '" + d.DSLocation
                + "', using create date " + d.DSCreateDT.getTime());
        return d.DSCreateDT.getTime();
    }

    private void showDatastreams(String pid, String context) {
        Get allDS = new Get(pid.getBytes());
        allDS.setMaxVersions();

        LOG.info(context + ": Showing managed streams of " + pid);
        try {
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap =
                    m_table.get().get(allDS).getMap();

            for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry : familyMap
                    .entrySet()) {
                String family = new String(familyEntry.getKey());
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : familyEntry
                        .getValue().entrySet()) {
                    String column = new String(columnEntry.getKey());
                    for (Map.Entry<Long, byte[]> versionEntry : columnEntry
                            .getValue().entrySet()) {
                        long ts = versionEntry.getKey().longValue();
                        int size = versionEntry.getValue().length;

                        LOG.info(context + " In storage: " + family + ":"
                                + column + " ts=" + ts + ", size=" + size);
                    }
                }
            }

            DigitalObject obj = _readObject(pid);

            Iterator<String> dsIDs = obj.datastreamIdIterator();

            while (dsIDs.hasNext()) {
                for (Datastream d : obj.datastreams(dsIDs.next())) {
                    LOG.info(context + " In object: " + d.DatastreamID + "+"
                            + d.DSVersionID + " (" + d.DSControlGrp + ")"
                            + ", ds=" + d.DSCreateDT.getTime());
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DigitalObject _readObject(String objectKey) throws IOException {
        Get obj = new Get(objectKey.getBytes());
        obj.addColumn(Family.OBJECT, Column.CONTENT);
        obj.addColumn(Family.OBJECT, Column.FORMAT);
        obj.addColumn(Family.OBJECT, Column.ENCODING);

        Result row = m_table.get().get(obj);

        /* If the format and encoding aren't in the table, use the defaults */
        byte[] encoding = row.getValue(Family.OBJECT, Column.ENCODING);
        byte[] format = row.getValue(Family.OBJECT, Column.FORMAT);

        try {
            return fixDSLocations(deserialize(row.getValue(Family.OBJECT,
                                                           Column.CONTENT),
                                              format != null ? new String(format)
                                                      : m_format,
                                              encoding != null ? new String(encoding)
                                                      : m_encoding));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initTable() throws ModuleInitializationException {
        try {
            HBaseAdmin hbase = new HBaseAdmin(m_hbase_conf);
            HTableDescriptor desc = new HTableDescriptor(m_tableName);
            desc.addFamily(new HColumnDescriptor("object".getBytes()));
            desc.addFamily(new HColumnDescriptor("datastream".getBytes()));
            desc.addFamily(new HColumnDescriptor("meta".getBytes()));
            hbase.createTable(desc);
        } catch (Exception e) {
            throw new ModuleInitializationException("Could not create new HBase table",
                                                    getRole(),
                                                    e);
        }
    }

    public interface Column {

        static final byte[] CONTENT = "content".getBytes();

        static final byte[] FORMAT = "format".getBytes();

        static final byte[] ENCODING = "encoding".getBytes();

    }

    public interface Family {

        static final byte[] OBJECT = "object".getBytes();

        static final byte[] DATASTREAM = "datastream".getBytes();
    }
}