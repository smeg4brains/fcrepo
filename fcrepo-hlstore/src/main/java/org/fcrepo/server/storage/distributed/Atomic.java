
package org.fcrepo.server.storage.distributed;

import org.fcrepo.server.errors.LowlevelStorageException;
import org.fcrepo.server.errors.ObjectNotInLowlevelStorageException;
import org.fcrepo.server.storage.highlevel.HighlevelStorage;
import org.fcrepo.server.storage.highlevel.OutOfDateException;
import org.fcrepo.server.storage.types.DigitalObject;

/**
 * Marker interface for {@link HighlevelStorage} that performs atomic writes.
 */
public interface Atomic {

    /**
     * Stores object and managed datastream versions only if there is no stored
     * version of the object.
     * <p>
     * The existence test, and FOXML + datastream version additions occur as a
     * single atomic operation.
     *</p>
     *
     * @param object
     *        DigitalObject containing new object metadata and managed content
     *        streams.
     * @throws ObjectNotInLowlevelStorageException
     *         thrown when an object with the same PID already exists in storage
     */
    public void add(DigitalObject object) throws LowlevelStorageException;

    /**
     * Compares to old version, updates FOXML and adds or removes datastream
     * versions from storage in a single atomic operation.
     * <p>
     * All comparisons/verifications, updates, and removals are to occur as a
     * single atomic operation.
     *</p>
     *
     * @throws OutOfDateException
     *         thrown when the object in storage has changed since the given
     *         oldVersion object.
     */
    public void update(DigitalObject oldVersion, DigitalObject newVersion)
            throws LowlevelStorageException;
}
