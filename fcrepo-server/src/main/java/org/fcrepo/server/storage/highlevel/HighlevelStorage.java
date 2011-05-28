
package org.fcrepo.server.storage.highlevel;

import java.io.InputStream;

import org.fcrepo.server.errors.LowlevelStorageException;
import org.fcrepo.server.errors.ObjectNotInLowlevelStorageException;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DigitalObject;

/**
 * High-level storage interface for intelligent datastores.
 * <p>
 * Presents simple, high level storage interface that allows underlying storage
 * implementations to decide how to serialize and store the data contained in
 * objects.
 * </p>
 * <p>
 * DOManagers or other code using <code>HighLevelStorage</code> are responsible
 * for managing and interpreting the content of DigitalObjects (including
 * tracking/namining new datastream versions, validation, etc), as well as
 * serialization for export or archival purposes. <code>HighLevelStorage</code>
 * is responsible for serializing and storing the object by whatever means and
 * in whatever format it sees fit. The only guarantee is that the
 * {@link DigitalObject} retrieved from storage will be logically equivalent to
 * the <code>DigitalObject</code> last stored, with one exception:
 * <ul>
 * <li>For any given datastream, {@link Datastream#DSLocation} or
 * {@link Datastream#DSLocationType} may, at the option of the underlying
 * storage implementation, be set or modified to suit its own internal storage
 * needs.</li>
 * </ul>
 * </p>
 */
public interface HighlevelStorage {

    /**
     * Creates FOXML and adds datastream versions only if there is no stored
     * version of the object.
     *
     * @param object
     *        DigitalObject containing new object metadata and managed content
     *        streams.
     * @throws ObjectNotInLowlevelStorageException
     *         thrown when an object with the same PID already exists in storage
     */
    public void add(DigitalObject object) throws LowlevelStorageException;

    /**
     * Updates FOXML and adds or removes datastream versions from storage.
     *
     * @throws OutOfDateException
     *         thrown when the object in storage has changed since the given
     *         oldVersion object.
     */
    public void update(DigitalObject oldVersion, DigitalObject newVersion)
            throws LowlevelStorageException;

    /**
     * Reads an object from storage.
     *
     * @param pid
     *        PID of object to read.
     * @return Fully populated DigitalObject
     * @throws LowlevelStorageException
     */
    public DigitalObject readObject(String pid) throws LowlevelStorageException;

    public InputStream readDatastream(DigitalObject object,
                                      Datastream datastream)
            throws LowlevelStorageException;

    /**
     * Remove FOXML and all managed datastreams from storage.
     *
     * @param pid
     *        PID of the object to remove.
     */
    public void purge(String pid) throws LowlevelStorageException;

    public boolean exists(String pid) throws LowlevelStorageException;
}
