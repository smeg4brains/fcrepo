package org.fcrepo.server.storage.highlevel.akubra;

import java.io.InputStream;

import org.fcrepo.server.errors.LowlevelStorageException;
import org.fcrepo.server.storage.highlevel.HighlevelStorage;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DigitalObject;


public class AkubraHighlevelStorage
        implements HighlevelStorage {

    public DigitalObject readObject(String pid) throws LowlevelStorageException {
        // TODO Auto-generated method stub
        return null;
    }

    public InputStream readDatastream(DigitalObject object,
                                      Datastream datastream)
            throws LowlevelStorageException {
        // TODO Auto-generated method stub
        return null;
    }

    public void purge(String pid) throws LowlevelStorageException {
        // TODO Auto-generated method stub

    }

    public boolean exists(String pid) throws LowlevelStorageException {
        // TODO Auto-generated method stub
        return false;
    }

    public void add(DigitalObject object) throws LowlevelStorageException {
        // TODO Auto-generated method stub

    }

    public void update(DigitalObject oldVersion, DigitalObject newVersion)
            throws LowlevelStorageException {
        // TODO Auto-generated method stub

    }

}
