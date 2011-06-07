/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */

package org.fcrepo.server.storage.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import java.util.Date;
import java.util.Iterator;
import java.util.Set;

import org.fcrepo.server.Context;
import org.fcrepo.server.RecoveryContext;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.GeneralException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.management.PIDGenerator;
import org.fcrepo.server.storage.DOWriter;
import org.fcrepo.server.storage.translation.DOTranslationUtility;
import org.fcrepo.server.storage.translation.DOTranslator;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DatastreamXMLMetadata;
import org.fcrepo.server.storage.types.DigitalObject;
import org.fcrepo.server.storage.types.DigitalObjectUtil;
import org.fcrepo.server.utilities.DCField;
import org.fcrepo.server.utilities.DCFields;
import org.fcrepo.server.utilities.StreamUtility;
import org.fcrepo.server.validation.DOValidator;
import org.fcrepo.server.validation.DOValidatorImpl;
import org.fcrepo.server.validation.ValidationUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.fcrepo.common.Constants.FOXML1_0;
import static org.fcrepo.common.Constants.FOXML1_0_LEGACY;
import static org.fcrepo.common.Constants.METS_EXT1_0;
import static org.fcrepo.common.Constants.METS_EXT1_0_LEGACY;
import static org.fcrepo.common.Constants.OAI_DC2_0;
import static org.fcrepo.common.Constants.RECOVERY;

/**
 * Utility class for populating/building and validating new objects for ingest.
 * <p>
 * Given user ingest data (FOXML or other), this class will populate a DOWriter
 * with the requested data, as well as provide any additional default data
 * required for ingest and perform basic validation.
 * </p>
 *
 * @author Aaron Birkland
 * @version $Id$
 */
public class ObjectBuilder {

    private final DOTranslator m_translator;

    private final DOValidator m_validator;

    private final Set<String> m_retainPIDs;

    private final PIDGenerator m_pidGenerator;

    private final String m_pidNamespace;

    public ObjectBuilder(DOTranslator translator,
                         DOValidator validator,
                         PIDGenerator pidGenerator,
                         Set<String> retainPids,
                         String pidNamespace) {
        m_translator = translator;
        m_validator = validator;
        m_pidGenerator = pidGenerator;
        m_retainPIDs = retainPids;
        m_pidNamespace = pidNamespace;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ObjectBuilder.class);

    /**
     * Populate a new object with given data.
     *
     * @param writer
     *        DOWriter into which object data will be written.
     * @param context
     *        Fedora server context.
     * @param in
     *        Serialization of new object.
     * @param format
     *        Format of serialized input object.
     * @param encoding
     *        Encoding of serialized input data.
     * @param newPid
     *        boolean true if a new pid should be generated and assigned to the
     *        object.
     * @throws ServerException
     */
    public void populateNew(DOWriter writer,
                            Context context,
                            InputStream in,
                            String format,
                            String encoding,
                            String newPid) throws ServerException {

        DigitalObject obj = writer.getObject();
        try {
            // CURRENT TIME:
            // Get the current time to use for created dates on object
            // and object components (if they are not already there).
            Date nowUTC = Server.getCurrentDate(context);

            // TEMP STORAGE:
            // write ingest input stream to a temporary file
            File tempFile = File.createTempFile("fedora-ingest-temp", ".xml");
            LOG.debug("Creating temporary file for ingest: "
                    + tempFile.toString());
            StreamUtility.pipeStream(in, new FileOutputStream(tempFile), 4096);

            // VALIDATION:
            // perform initial validation of the ingest submission file
            LOG.debug("Validation (ingest phase)");
            m_validator.validate(tempFile,
                                 format,
                                 DOValidatorImpl.VALIDATE_ALL,
                                 "ingest");

            // DESERIALIZE:
            // deserialize the ingest input stream into a digital object instance
            obj.setNew(true);
            LOG.debug("Deserializing from format: " + format);
            m_translator.deserialize(new FileInputStream(tempFile),
                                     obj,
                                     format,
                                     encoding,
                                     DOTranslationUtility.DESERIALIZE_INSTANCE);

            // SET OBJECT PROPERTIES:
            LOG
                    .debug("Setting object/component states and create dates if unset");
            // set object state to "A" (Active) if not already set
            if (obj.getState() == null || obj.getState().equals("")) {
                obj.setState("A");
            }
            // set object create date to UTC if not already set
            if (obj.getCreateDate() == null || obj.getCreateDate().equals("")) {
                obj.setCreateDate(nowUTC);
            }
            // set object last modified date to UTC
            obj.setLastModDate(nowUTC);

            // SET DATASTREAM PROPERTIES...
            Iterator<String> dsIter = obj.datastreamIdIterator();
            while (dsIter.hasNext()) {
                for (Datastream ds : obj.datastreams(dsIter.next())) {
                    // Set create date to UTC if not already set
                    if (ds.DSCreateDT == null || ds.DSCreateDT.equals("")) {
                        ds.DSCreateDT = nowUTC;
                    }
                    // Set state to "A" (Active) if not already set
                    if (ds.DSState == null || ds.DSState.equals("")) {
                        ds.DSState = "A";
                    }
                    ds.DSChecksumType =
                            Datastream.validateChecksumType(ds.DSChecksumType);
                }
            }

            // SET MIMETYPE AND FORMAT_URIS FOR LEGACY OBJECTS' DATASTREAMS
            if (FOXML1_0.uri.equals(format) || FOXML1_0_LEGACY.equals(format)
                    || METS_EXT1_0.uri.equals(format)
                    || METS_EXT1_0_LEGACY.equals(format)) {
                DigitalObjectUtil.updateLegacyDatastreams(obj);
            }

            // PID VALIDATION:
            // validate and normalized the provided pid, if any
            if (obj.getPid() != null && obj.getPid().length() > 0) {
                obj.setPid(Server.getPID(obj.getPid()).toString());
            }

            // PID GENERATION:
            // have the system generate a PID if one was not provided
            if (obj.getPid() != null
                    && obj.getPid().indexOf(":") != -1
                    && (m_retainPIDs == null || m_retainPIDs.contains(obj
                            .getPid().split(":")[0]))) {
                LOG
                        .debug("Stream contained PID with retainable namespace-id; will use PID from stream");
                try {
                    m_pidGenerator.neverGeneratePID(obj.getPid());
                } catch (IOException e) {
                    throw new GeneralException("Error calling pidGenerator.neverGeneratePID(): "
                            + e.getMessage());
                }
            } else {
                if (newPid == null || "new".equals(newPid)) {
                    LOG.debug("Client wants a new PID");
                    // yes... so do that, then set it in the obj.
                    String p = null;
                    try {
                        // If the context contains a recovery PID, use that.
                        // Otherwise, generate a new PID as usual.
                        if (context instanceof RecoveryContext) {
                            RecoveryContext rContext =
                                    (RecoveryContext) context;
                            p = rContext.getRecoveryValue(RECOVERY.PID.uri);
                        }
                        if (p == null) {
                            p =
                                    m_pidGenerator.generatePID(m_pidNamespace)
                                            .toString();
                        } else {
                            LOG.debug("Using new PID from recovery context");
                            m_pidGenerator.neverGeneratePID(p);
                        }
                    } catch (Exception e) {
                        throw new GeneralException("Error generating PID, PIDGenerator returned unexpected error: ("
                                + e.getClass().getName()
                                + ") - "
                                + e.getMessage());
                    }
                    LOG.info("Generated new PID: " + p);
                    obj.setPid(p);
                } else {
                    LOG.debug("Client wants to use existing PID.");
                }
            }

            LOG.debug("New object PID is " + obj.getPid());

            // DEFAULT DATASTREAMS:
            populateDC(obj, writer, nowUTC);

            // DATASTREAM VALIDATION
            ValidationUtility.validateReservedDatastreams(writer);

        } catch (Exception e) {
            if (e instanceof ServerException) {
                throw (ServerException) e;
            } else {
                throw new GeneralException("Could not ingest object", e);
            }
        }
    }

    /**
     * Adds a minimal DC datastream if one isn't already present. If there is
     * already a DC datastream, ensure one of the dc:identifier values is the
     * PID of the object.
     */
    private static void populateDC(DigitalObject obj, DOWriter w, Date nowUTC)
            throws IOException, ServerException {
        LOG.debug("Adding/Checking default DC datastream");
        DatastreamXMLMetadata dc =
                (DatastreamXMLMetadata) w.GetDatastream("DC", null);
        DCFields dcf;
        if (dc == null) {
            dc = new DatastreamXMLMetadata("UTF-8");
            dc.DSMDClass = 0;
            //dc.DSMDClass=DatastreamXMLMetadata.DESCRIPTIVE;
            dc.DatastreamID = "DC";
            dc.DSVersionID = "DC1.0";
            dc.DSControlGrp = "X";
            dc.DSCreateDT = nowUTC;
            dc.DSLabel = "Dublin Core Record for this object";
            dc.DSMIME = "text/xml";
            dc.DSFormatURI = OAI_DC2_0.uri;
            dc.DSSize = 0;
            dc.DSState = "A";
            dc.DSVersionable = true;
            dcf = new DCFields();
            if (obj.getLabel() != null && !obj.getLabel().equals("")) {
                dcf.titles().add(new DCField(obj.getLabel()));
            }
            w.addDatastream(dc, dc.DSVersionable);
        } else {
            dcf = new DCFields(new ByteArrayInputStream(dc.xmlContent));
        }
        // ensure one of the dc:identifiers is the pid
        boolean sawPid = false;
        for (DCField dcField : dcf.identifiers()) {
            if (dcField.getValue().equals(obj.getPid())) {
                sawPid = true;
            }
        }
        if (!sawPid) {
            dcf.identifiers().add(new DCField(obj.getPid()));
        }
        // set the value of the dc datastream according to what's in the DCFields object
        try {
            dc.xmlContent = dcf.getAsXML().getBytes("UTF-8");
        } catch (UnsupportedEncodingException uee) {
            // safely ignore... we know UTF-8 works
        }
    }
}
