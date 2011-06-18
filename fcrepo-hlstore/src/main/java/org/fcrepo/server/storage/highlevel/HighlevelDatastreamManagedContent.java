
package org.fcrepo.server.storage.highlevel;

import java.io.InputStream;

import org.fcrepo.server.Context;
import org.fcrepo.server.ReadOnlyContext;
import org.fcrepo.server.errors.StreamIOException;
import org.fcrepo.server.errors.ValidationException;
import org.fcrepo.server.management.Management;
import org.fcrepo.server.storage.ContentManagerParams;
import org.fcrepo.server.storage.ExternalContentManager;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DatastreamManagedContent;
import org.fcrepo.server.storage.types.DigitalObject;
import org.fcrepo.server.storage.types.MIMETypedStream;
import org.fcrepo.server.validation.ValidationUtility;

/**
 * Datastream where content is pulled from {@link HighlevelStorage}
 * <p>
 * This class extends DatastreamManagedContent because DefaultAccess will cast
 * it as such.
 * </p>
 */
public class HighlevelDatastreamManagedContent
        extends DatastreamManagedContent {

    private final HighlevelStorage m_store;

    private final DigitalObject m_object;

    private final Datastream m_datastreamDef;

    private final Management m_mgmt;

    private final ExternalContentManager m_ecm;

    public HighlevelDatastreamManagedContent(DigitalObject obj,
                                             Datastream ds,
                                             HighlevelStorage store,
                                             Management mgmt,
                                             ExternalContentManager ecm) {
        ds.copy(this);
        m_store = store;
        m_object = obj;
        m_datastreamDef = ds;
        m_mgmt = mgmt;
        m_ecm = ecm;
    }

    @Override
    public InputStream getContentStream() throws StreamIOException {
        try {
            // For new or modified datastreams, the new bytestream hasn't yet
            // been
            // committed. However, we need to access it in order to compute
            // the datastream checksum
            if (DSLocation.startsWith(UPLOADED_SCHEME)) {
                return m_mgmt.getTempStream(DSLocation);
            } else if (DSLocation.contains(":/")) {
                try {

                    ValidationUtility
                            .validateURL(DSLocation, this.DSControlGrp);
                    // If validation has succeeded, assume an external resource.
                    // Fetch it, store it locally, update DSLocation
                    Context ctx = ReadOnlyContext.EMPTY;
                    MIMETypedStream stream =
                            m_ecm.getExternalContent(new ContentManagerParams(DSLocation));
                    DSLocation = m_mgmt.putTempStream(ctx, stream.getStream());
                    return m_mgmt.getTempStream(DSLocation);
                } catch (ValidationException e) {
                    // Just in case an internal DSLocation ever looks like a
                    // URI...
                    return m_store.readDatastream(m_object, m_datastreamDef);
                }
            } else {
                return m_store.readDatastream(m_object, m_datastreamDef);
            }
        } catch (Throwable th) {
            throw new StreamIOException("Could not read content stream", th);
        }
    }
}
