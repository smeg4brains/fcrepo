
package org.fcrepo.server.management;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.fcrepo.common.PID;
import org.fcrepo.server.Module;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.management.PIDGenerator;

/** Simply generates a random PID using a a UUID. */
public class UUIDPidGenerator
        extends Module
        implements PIDGenerator {

    public UUIDPidGenerator(Map<String, String> moduleParameters,
                            Server server,
                            String role)
            throws ModuleInitializationException {
        super(moduleParameters, server, role);
    }

    public PID generatePID(String namespace) throws IOException {
        return PID.getInstance(namespace + ":" + UUID.randomUUID());
    }

    public PID getLastPID() throws IOException {
        throw new UnsupportedOperationException();
    }

    public void neverGeneratePID(String pid) throws IOException {
        return;
    }

}
