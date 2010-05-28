/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.journal.managementmethods;

import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.journal.entry.JournalEntry;
import org.fcrepo.server.management.ManagementDelegate;


/**
 * Adapter class for Management.modifyDatastreamByReference()
 *
 * @author Jim Blake
 */
public class ModifyDatastreamByReferenceMethod
        extends ManagementMethod {

    public ModifyDatastreamByReferenceMethod(JournalEntry parent) {
        super(parent);
    }

    @Override
    public Object invoke(ManagementDelegate delegate) throws ServerException {
        return delegate.modifyDatastreamByReference(parent.getContext(), parent
                .getStringArgument(ARGUMENT_NAME_PID), parent
                .getStringArgument(ARGUMENT_NAME_DS_ID), parent
                .getStringArrayArgument(ARGUMENT_NAME_ALT_IDS), parent
                .getStringArgument(ARGUMENT_NAME_DS_LABEL), parent
                .getStringArgument(ARGUMENT_NAME_MIME_TYPE), parent
                .getStringArgument(ARGUMENT_NAME_FORMAT_URI), parent
                .getStringArgument(ARGUMENT_NAME_DS_LOCATION), parent
                .getStringArgument(ARGUMENT_NAME_CHECKSUM_TYPE), parent
                .getStringArgument(ARGUMENT_NAME_CHECKSUM), parent
                .getStringArgument(ARGUMENT_NAME_LOG_MESSAGE), parent
                .getDateArgument(ARGUMENT_NAME_LAST_MODIFIED_DATE));
    }

}
