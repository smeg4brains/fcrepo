
package org.fcrepo.server.storage.highlevel;

import org.fcrepo.server.errors.LowlevelStorageException;

/**
 * Exception indicating that the object has been modified in low level storage
 * by some other process or thread.
 *
 */
public class OutOfDateException
        extends LowlevelStorageException {

    public OutOfDateException(boolean serverCaused,
                              String message,
                              Throwable cause) {
        super(serverCaused, message, cause);
    }

    public OutOfDateException(boolean serverCaused, String message) {
        super(serverCaused, message, null);
    }
}
