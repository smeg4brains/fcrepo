
package org.fcrepo.server.storage.distributed;

import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.fcrepo.server.Context;
import org.fcrepo.server.Module;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.GeneralException;
import org.fcrepo.server.errors.InvalidContextException;
import org.fcrepo.server.errors.LowlevelStorageException;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.errors.StorageDeviceException;
import org.fcrepo.server.management.PIDGenerator;
import org.fcrepo.server.search.FieldSearchQuery;
import org.fcrepo.server.search.FieldSearchResult;
import org.fcrepo.server.search.ObjectFields;
import org.fcrepo.server.storage.DOManager;
import org.fcrepo.server.storage.DOReader;
import org.fcrepo.server.storage.DOReaderCache;
import org.fcrepo.server.storage.DOWriter;
import org.fcrepo.server.storage.ServiceDefinitionReader;
import org.fcrepo.server.storage.ServiceDeploymentReader;
import org.fcrepo.server.storage.SimpleServiceDefinitionReader;
import org.fcrepo.server.storage.SimpleServiceDeploymentReader;
import org.fcrepo.server.storage.highlevel.Atomic;
import org.fcrepo.server.storage.highlevel.HighlevelStorage;
import org.fcrepo.server.storage.management.ObjectBuilder;
import org.fcrepo.server.storage.translation.DOTranslator;
import org.fcrepo.server.validation.DOValidator;


/**
 * DOManager that is safe to use with multiple writers to a single object store.
 * <p>
 * This DOManager is designed function in a distributed system of multiple
 * writers (such as multiple Fedora instances writing to the same store). This
 * is achieved by the following:
 * <dl>
 * <dt>No local state</dt>
 * <dd>Does not maintain its own object/datastream registry, nor does it
 * maintain any sort of shared index/registry with other Fedora instances. Every
 * operation simply reads or writes to durable storage.</dd>
 * <dt>Atomic operations</dt>
 * <dd>Uses {@link HighlevelStorage} where operations involving checks and
 * updates to FOXML and managed datastreams occur atomically</dd>
 * <dt>Monotonicity of changes</dt>
 * <dd>As per the Fedora model, mutations to a fedora object are timestamped,
 * and changes to Fedora objects that violate a strict ordering of time are not
 * allowed. No form of clock synchronization is assumed between writers, so
 * although timestamps from any one writer are considered arbitrary, a
 * DistributedDOManager preserves thie timestamp monotinicity on a per-object
 * scope.</dd>
 * </dl>
 * </p>
 * <p>
 * There are, however, a few facts to be aware of:
 * <p>
 * <ul>
 * <li>Secondary indexing services such as Field Search or the Resource Index
 * are currently disabled. TODO: implement as external services that are updated
 * asynchronously by messaging, or polling the data on their own.</li>
 * <li>{@link PIDManager} is a new interface for managing pid generation. In a
 * cluster of multiple Fedora instances, the <code>PIDManager</code>
 * implementation will either have to coordinate pid generation amongst the
 * various fedora instance, or each instance will need to be individually
 * configured in some way that results in unique pids (e.g. using
 * location-specific prefixes, hash values, etc).</li>
 * <li>{@link DeploymentManager} is a new interface for managing the mapping
 * between CModels and SDeps. The SDep implementation of a service for a CModel
 * can be changed by any Fedora instance at any time, so
 * <code>DeploymentManager</code> is responsible for getting a local Fedora
 * instance updated as quickly as possible.
 * </ul>
 * </p>
 *
 * @author birkland
 */
public class DistributedDOManager
        extends Module
        implements DOManager {

    private static final Logger LOG =
            Logger.getLogger(DistributedDOManager.class);

    private PIDGenerator m_pidGenerator;

    private DeploymentManager m_deploymentManager;

    private DOValidator m_validator;

    private DistributedObjectSource m_access;

    private DOReaderCache m_readerCache;

    private DOTranslator m_translator;

    private String m_defaultExportFormat;

    private String m_pidNamespace;

    private String m_encoding;

    private ObjectBuilder m_builder;

    private HighlevelStorage m_permanentStore;

    Set<String> m_retainPIDs;

    public DistributedDOManager(Map<String, String> moduleParameters,
                                Server server,
                                String role)
            throws ModuleInitializationException {
        super(moduleParameters, server, role);

    }

    @Override
    public void postInitModule() throws ModuleInitializationException {

        m_pidGenerator =
                (PIDGenerator) getServer()
                        .getModule("org.fcrepo.server.management.PIDGenerator");
        m_validator =
                (DOValidator) getServer()
                        .getModule("org.fcrepo.server.validation.DOValidator");
        if (m_validator == null) {
            throw new ModuleInitializationException("DOValidator not loaded.",
                                                    getRole());
        }

        m_deploymentManager =
                (DeploymentManager) getServer()
                        .getModule(DeploymentManager.class.getName());

        m_defaultExportFormat = getParameter("defaultExportFormat");
        if (m_defaultExportFormat == null) {
            throw new ModuleInitializationException("Parameter defaultExportFormat "
                                                            + "not given, but it's required.",
                                                    getRole());
        }

        m_translator =
                (DOTranslator) getServer()
                        .getModule("org.fcrepo.server.storage.translation.DOTranslator");

        try {
            m_permanentStore =
                    (HighlevelStorage) getServer()
                            .getModule(HighlevelStorage.class.getName());
            if (m_permanentStore == null) {
                throw new ModuleInitializationException("HighlevelStorage not loaded",
                                                        getRole());
            }

            if (!(m_permanentStore instanceof Atomic)) {
                throw new ModuleInitializationException("High level storage must support atomic writes",
                                                        getRole());
            }
        } catch (ClassCastException e) {
            throw new ModuleInitializationException("Storage "
                    + m_permanentStore.getClass()
                    + " is not an instance of AtomicLowLevelStorage", getRole());
        }

        m_encoding = getParameter("storageCharacterEncoding");
        if (m_encoding == null) {
            m_encoding = "UTF-8";
        }

        initReaderCache();
        getPIDNamespace();

        m_access =
                new DistributedObjectSource(m_translator,
                                            m_defaultExportFormat,
                                            m_encoding,
                                            m_permanentStore);
        m_builder =
                new ObjectBuilder(m_translator,
                                  m_validator,
                                  m_pidGenerator,
                                  m_retainPIDs,
                                  m_pidNamespace);
    }

    public FieldSearchResult findObjects(Context context,
                                         String[] resultFields,
                                         int maxResults,
                                         FieldSearchQuery query)
            throws ServerException {
        /*
         * TODO: Write a FieldSearch impl that uses an external service/index
         * (such as lucene), as distributed Fedora instances should not be
         * maintaining their own indexes
         */

        return new EmptyResult();
    }

    @Override
    public DOWriter getIngestWriter(boolean cachedObjectRequired,
                                    Context context,
                                    InputStream in,
                                    String format,
                                    String encoding,
                                    String newPid) throws ServerException {
        if (cachedObjectRequired) {
            throw new InvalidContextException("A DOWriter is unavailable in a cached context.");
        }

        DOWriter writer = m_access.newObject();

        m_builder.populateNew(writer, context, in, format, encoding, newPid);

        return writer;
    }
    public String[] getNextPID(int numPIDs, String namespace)
            throws ServerException {
        if (numPIDs < 1) {
            numPIDs = 1;
        }
        String[] pidList = new String[numPIDs];
        if (namespace == null || namespace.equals("")) {
            namespace = m_pidNamespace;
        }
        try {
            for (int i = 0; i < numPIDs; i++) {
                pidList[i] = m_pidGenerator.generatePID(namespace).toString();
            }
            return pidList;
        } catch (IOException ioe) {
            throw new GeneralException("Error generating PID", ioe);
        }
    }

    public String getRepositoryHash() throws ServerException {
        throw new UnsupportedOperationException("Will not calculate hash "
                + "of distributed repository");
    }

    public DOWriter getWriter(boolean cachedObjectRequired,
                              Context context,
                              String pid) throws ServerException {
        return m_access.fetchObject(pid, context);
    }

    public String lookupDeploymentForCModel(String cModelPid, String sDefPid) {
        return m_deploymentManager.lookupDeployment(cModelPid, sDefPid);
    }

    public boolean objectExists(String pid) throws StorageDeviceException {
        try {
            return m_permanentStore.exists(pid);
        } catch (LowlevelStorageException e) {
            throw new StorageDeviceException("Could not determine if object exists",
                                             e);
        }
    }

    public void releaseWriter(DOWriter writer) throws ServerException {
        /* Since this implementation does not use local locking, this is a noop */
    }

    public void reservePIDs(String[] pidList) throws ServerException {
        try {
            for (String element : pidList) {
                m_pidGenerator.neverGeneratePID(element);
            }
        } catch (IOException e) {
            throw new GeneralException("Error reserving PIDs", e);
        }
    }

    public FieldSearchResult resumeFindObjects(Context context,
                                               String sessionToken)
            throws ServerException {
        return new EmptyResult();
    }

    public DOReader getReader(boolean cachedObjectRequired,
                              Context context,
                              String pid) throws ServerException {
        DOReader cachedRreader = null;
        if (cachedObjectRequired) {
            if (m_readerCache != null) {
                cachedRreader = m_readerCache.get(pid);
            }
        }

        if (cachedRreader == null) {
            return m_access.fetchObject(pid, context);
        } else {
            return cachedRreader;
        }
    }

    public ServiceDefinitionReader getServiceDefinitionReader(boolean cachedObjectRequired,
                                                              Context context,
                                                              String pid)
            throws ServerException {
        return new SimpleServiceDefinitionReader(context,
                                                 this,
                                                 m_translator,
                                                 m_defaultExportFormat,
                                                 m_encoding,
                                                 m_permanentStore
                                                         .readObject(pid));
    }

    public ServiceDeploymentReader getServiceDeploymentReader(boolean cachedObjectRequired,
                                                              Context context,
                                                              String pid)
            throws ServerException {
        return new SimpleServiceDeploymentReader(context,
                                                 this,
                                                 m_translator,
                                                 m_defaultExportFormat,
                                                 m_encoding,
                                                 m_permanentStore
                                                         .readObject(pid));
    }

    public String[] listObjectPIDs(Context context) throws ServerException {
        throw new UnsupportedOperationException("List all pids as an in-memory array?  "
                + "Are you crazy??");
    }

    private void initReaderCache() throws ModuleInitializationException {
        // readerCacheSize and readerCacheSeconds (optional, defaults = 20, 5)
        String rcSize = getParameter("readerCacheSize");
        if (rcSize == null) {
            LOG.debug("Parameter readerCacheSize not given, using 20");
            rcSize = "20";
        }
        int readerCacheSize;
        try {
            readerCacheSize = Integer.parseInt(rcSize);
            if (readerCacheSize < 0) {
                throw new Exception("Cannot be less than zero");
            }
        } catch (Exception e) {
            throw new ModuleInitializationException("Bad value for readerCacheSize parameter: "
                                                            + e.getMessage(),
                                                    getRole());
        }

        String rcSeconds = getParameter("readerCacheSeconds");
        if (rcSeconds == null) {
            LOG.debug("Parameter readerCacheSeconds not given, using 5");
            rcSeconds = "5";
        }
        int readerCacheSeconds;
        try {
            readerCacheSeconds = Integer.parseInt(rcSeconds);
            if (readerCacheSeconds < 1) {
                throw new Exception("Cannot be less than one");
            }
        } catch (Exception e) {
            throw new ModuleInitializationException("Bad value for readerCacheSeconds parameter: "
                                                            + e.getMessage(),
                                                    getRole());
        }

        if (readerCacheSize > 0) {
            m_readerCache =
                    new DOReaderCache(readerCacheSize, readerCacheSeconds);
        }
    }

    private void getPIDNamespace() throws ModuleInitializationException {
        // pidNamespace (required, 1-17 chars, a-z, A-Z, 0-9 '-' '.')
        m_pidNamespace = getParameter("pidNamespace");
        if (m_pidNamespace == null) {
            throw new ModuleInitializationException("pidNamespace parameter must be specified.",
                                                    getRole());
        }
        if (m_pidNamespace.length() > 17 || m_pidNamespace.length() < 1) {
            throw new ModuleInitializationException("pidNamespace parameter must be 1-17 chars long",
                                                    getRole());
        }
        StringBuffer badChars = new StringBuffer();
        for (int i = 0; i < m_pidNamespace.length(); i++) {
            char c = m_pidNamespace.charAt(i);
            boolean invalid = true;
            if (c >= '0' && c <= '9') {
                invalid = false;
            } else if (c >= 'a' && c <= 'z') {
                invalid = false;
            } else if (c >= 'A' && c <= 'Z') {
                invalid = false;
            } else if (c == '-') {
                invalid = false;
            } else if (c == '.') {
                invalid = false;
            }
            if (invalid) {
                badChars.append(c);
            }
        }
        if (badChars.toString().length() > 0) {
            throw new ModuleInitializationException("pidNamespace contains "
                                                            + "invalid character(s) '"
                                                            + badChars
                                                                    .toString()
                                                            + "'",
                                                    getRole());
        }
    }

    protected void initRetainPID() {
        m_retainPIDs = new HashSet<String>();
        String retainPIDs = getParameter("retainPIDs");
        if (retainPIDs == null || retainPIDs.equals("*")) {
            // when m_retainPIDS is set to null, that means "all"
            m_retainPIDs = null;
        } else {
            // add to list (accept space and/or comma-separated)
            String[] ns =
                    retainPIDs.trim().replaceAll(" +", ",").replaceAll(",+",
                                                                       ",")
                            .split(",");
            for (String element : ns) {
                if (element.length() > 0) {
                    m_retainPIDs.add(element);
                }
            }

            // fedora-system PIDs must be ingestable as-is
            m_retainPIDs.add("fedora-system");
        }
    }

    private class EmptyResult
            implements FieldSearchResult {

        public long getCompleteListSize() {
            return 0;
        }

        public long getCursor() {
            return 0;
        }

        public Date getExpirationDate() {
            return new Date(0);
        }

        public String getToken() {
            return "-";
        }

        public List<ObjectFields> objectFieldsList() {
            return new ArrayList<ObjectFields>();
        }
    }
}
