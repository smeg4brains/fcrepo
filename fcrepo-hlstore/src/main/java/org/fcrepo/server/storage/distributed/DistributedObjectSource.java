
package org.fcrepo.server.storage.distributed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import java.util.Iterator;
import java.util.Map;

import org.fcrepo.server.Context;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.GeneralException;
import org.fcrepo.server.errors.ObjectIntegrityException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.errors.StreamIOException;
import org.fcrepo.server.errors.UnsupportedTranslationException;
import org.fcrepo.server.storage.DOWriter;
import org.fcrepo.server.storage.SimpleDOWriter;
import org.fcrepo.server.storage.highlevel.HighlevelStorage;
import org.fcrepo.server.storage.translation.DOTranslationUtility;
import org.fcrepo.server.storage.translation.DOTranslator;
import org.fcrepo.server.storage.types.BasicDigitalObject;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DigitalObject;
import org.fcrepo.utilities.DateUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates or fetches object from atomic storage providing a safe
 * <code>commit()></code>.
 * <p>
 * Produces fully independent writers that are safe in distributed applications,
 * as specified in {@link DistributedDOManager}
 * </p>
 */
class DistributedObjectSource {

    private static final Logger LOG =
            LoggerFactory.getLogger(DistributedObjectSource.class);

    private final DOTranslator m_translator;

    private final HighlevelStorage m_storage;

    private final String m_defaultFormat;

    private final String m_encoding;

    /*
     * Used for keeping track of local context, i.e. multiple operations that
     * occur serially in the same thread using the same context, but with
     * different instances of DOWriter.
     */
    private final ThreadLocal<Context> m_LocalCxt = new ThreadLocal<Context>();

    public DistributedObjectSource(DOTranslator translator,
                                   String storageFormat,
                                   String encoding,
                                   HighlevelStorage storage) {
        m_translator = translator;
        m_storage = storage;
        m_defaultFormat = storageFormat;
        m_encoding = encoding;
    }

    public DOWriter newObject() {
        DigitalObject obj = new BasicDigitalObject();
        obj.setNew(true);
        return new DistributedObject(null, obj);
    }

    public DOWriter fetchObject(String pid, Context context) {
        try {
            return new DistributedObject(context, m_storage.readObject(pid));
        } catch (Exception e) {
            throw new RuntimeException("Could not load FOXML for " + pid, e);
        }
    }

    /*
     * Extends SimpleDOWriter to do all the manipulation accounting, and
     * provides its own atomic commit() method.
     */
    private class DistributedObject
            extends SimpleDOWriter {

        private boolean m_comitted = false;

        private boolean m_isValid = true;

        private boolean m_toDelete = false;

        private final DigitalObject m_origObject;

        private final Context m_context;

        private DistributedObject(Context context, DigitalObject obj) {
            super(context, null, m_translator, m_defaultFormat, m_encoding, obj);

            m_context = context;

            m_origObject = copy(obj);
        }

        @Override
        public void commit(String message) throws ServerException {
            assertValid();

            if (m_toDelete) {
                m_storage.purge(GetObjectPID());
            } else {

                /* Set last mod date to "now" */
                if (m_context != null) {
                    getObject()
                            .setLastModDate(Server.getCurrentDate(m_context));
                }

                /*
                 * Make certain that the new lastModifiedDate is greater than
                 * the object's last lastModifiedDate!
                 */
                if (!isNew()
                        && getObject().getLastModDate().getTime() <= m_origObject
                                .getLastModDate().getTime()) {

                    if (m_context != m_LocalCxt.get()) {
                        throw new GeneralException("Object was given an out of date timestamp.  "
                                + "Check the server clock, or try again."
                                + "Original timestamp = "
                                + DateUtility.convertDateToString(m_origObject
                                        .getLastModDate())
                                + ", New timestamp = "
                                + DateUtility.convertDateToString(getObject()
                                        .getLastModDate(), true));
                    } else {
                        LOG.info("Continuing operations in initial contet...");
                    }
                }

                /* Purge, Add, or Update as necessary */
                if (m_toDelete) {
                    m_storage.purge(getObject().getPid());
                } else if (getObject().isNew()) {
                    m_storage.add(getObject());
                } else {
                    m_storage.update(m_origObject, getObject());
                }

                m_LocalCxt.set(m_context);
            }

            m_comitted = true;
            invalidate();
        }

        @Override
        public void invalidate() {
            m_isValid = false;
        }

        @Override
        public boolean isCommitted() {
            return m_comitted;
        }

        @Override
        public void remove() {
            m_toDelete = true;
        }

        /*
         * This is needed because the constructor we need for
         * SimpleDOReader/Writer leaves the default storage format as NULL, so
         * GetObjecXML will always fail
         */
        @Override
        public InputStream GetObjectXML() throws ObjectIntegrityException,
                StreamIOException, UnsupportedTranslationException,
                ServerException {

            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            m_translator
                    .serialize(getObject(),
                               bytes,
                               m_defaultFormat,
                               "UTF-8",
                               DOTranslationUtility.SERIALIZE_STORAGE_INTERNAL);
            return new ByteArrayInputStream(bytes.toByteArray());
        }

        private void assertValid() throws ServerException {
            if (!m_isValid) {
                throw new ObjectIntegrityException("Object had been invalidated - can't commit");
            }
        }
    }

    private DigitalObject copy(DigitalObject obj) {
        DigitalObject copy = new BasicDigitalObject();
        copy.setCreateDate(obj.getCreateDate());
        copy.setLabel(obj.getLabel());
        copy.setLastModDate(obj.getLastModDate());
        copy.setNew(obj.isNew());
        copy.setOwnerId(obj.getOwnerId());
        copy.setPid(obj.getPid());
        copy.setState(obj.getState());

        copy.getAuditRecords().addAll(obj.getAuditRecords());
        for (Map.Entry<String, String> extProperty : obj.getExtProperties()
                .entrySet()) {
            copy.setExtProperty(extProperty.getKey(), extProperty.getValue());
        }

        Iterator<String> dsIDs = obj.datastreamIdIterator();

        while (dsIDs.hasNext()) {
            for (Datastream d : obj.datastreams(dsIDs.next())) {
                copy.addDatastreamVersion(d, true);
            }
        }
        return copy;
    }
}