
package org.fcrepo.server.storage.distributed;

import java.io.IOException;
import java.io.InputStream;

import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

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
import org.fcrepo.server.search.FieldSearch;
import org.fcrepo.server.search.FieldSearchQuery;
import org.fcrepo.server.search.FieldSearchResult;
import org.fcrepo.server.storage.DOManager;
import org.fcrepo.server.storage.DOReader;
import org.fcrepo.server.storage.DOReaderCache;
import org.fcrepo.server.storage.DOWriter;
import org.fcrepo.server.storage.ServiceDefinitionReader;
import org.fcrepo.server.storage.ServiceDeploymentReader;
import org.fcrepo.server.storage.SimpleServiceDefinitionReader;
import org.fcrepo.server.storage.SimpleServiceDeploymentReader;
import org.fcrepo.server.storage.highlevel.HighlevelStorage;
import org.fcrepo.server.storage.translation.DOTranslator;
import org.fcrepo.server.storage.util.ObjectBuilder;
import org.fcrepo.server.validation.DOValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

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
 * <li>In a cluster of multiple Fedora instances, the <code>PIDGenerator</code>
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

    private static final Logger LOG = LoggerFactory
            .getLogger(DistributedDOManager.class);

    private static final String DEFAULT_EXPORT_FORMAT = FOXML1_1.uri.toString();

    private static final String DEFAULT_CHARACTER_ENCODING = "UTF-8";

    private static final String DEFAULT_PID_NAMESPACE = "changeme";

    private PIDGenerator m_pidGenerator;

    private DeploymentManager m_deploymentManager;

    private DOValidator m_validator;

    private DistributedObjectSource m_access;

    private DOReaderCache m_readerCache;

    private DOTranslator m_translator;

    private FieldSearch m_fieldSearch;

    private String m_defaultExportFormat = DEFAULT_EXPORT_FORMAT;

    private String m_pidNamespace = DEFAULT_PID_NAMESPACE;

    private String m_encoding = DEFAULT_CHARACTER_ENCODING;

    private ObjectBuilder m_builder;

    private HighlevelStorage m_permanentStore;

    Set<String> m_retainPIDs;

    @Required
    public void setPIDGenerator(PIDGenerator pidg) {
        m_pidGenerator = pidg;
    }

    @Required
    public void setDOValidator(DOValidator dov) {
        m_validator = dov;
    }

    @Required
    public void setDeploymentManager(DeploymentManager dm) {
        m_deploymentManager = dm;
    }

    @Required
    public void setDOTranslator(DOTranslator dot) {
        m_translator = dot;
    }

    @Required
    public void setHighlevelStorage(HighlevelStorage hs) {
        m_permanentStore = hs;
    }

    @Required
    public void setFieldSearch(FieldSearch fs) {
        m_fieldSearch = fs;
    }

    public void setDOReaderCache(DOReaderCache dorc) {
        m_readerCache = dorc;
    }

    public void setDefaultExportFormat(String fmt) {
        m_defaultExportFormat = fmt;
    }

    public void setCharacterEncoding(String ce) {
        m_encoding = ce;
    }

    public void setDefaultPidNamespace(String dpn) {
        m_pidNamespace = dpn;
    }

    public void setRetainPids(Set<String> rp) {
        m_retainPIDs = rp;
    }

    public DistributedDOManager(Map<String, String> moduleParameters,
                                Server server,
                                String role)
            throws ModuleInitializationException {
        super(moduleParameters, server, role);

    }

    @Override
    @PostConstruct
    public void initModule() throws ModuleInitializationException {

        if (!(m_permanentStore instanceof Atomic)) {
            throw new ModuleInitializationException("High level storage must support atomic writes",
                                                    getRole());
        }

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
        return m_fieldSearch.findObjects(resultFields, maxResults, query);
    }

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
        return m_fieldSearch.resumeFindObjects(sessionToken);
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

    private void getPIDNamespace() throws ModuleInitializationException {
        // pidNamespace (required, 1-17 chars, a-z, A-Z, 0-9 '-' '.')
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
                                                            + badChars.toString()
                                                            + "'",
                                                    getRole());
        }
    }

    protected void initRetainPID() {
        if (m_retainPIDs != null) {
            // fedora-system PIDs must be ingestable as-is
            m_retainPIDs.add("fedora-system");
        }
    }
}
