/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://www.fedora.info/license/).
 */

package org.fcrepo.server.storage.highlevel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DigitalObject;

public class ManagedDatastreamComparator {

    private final List<Datastream> m_added = new ArrayList<Datastream>();

    private final List<Datastream> m_removed = new ArrayList<Datastream>();

    public ManagedDatastreamComparator(DigitalObject newObject) {
        compare(new HashMap<String, Datastream>(), getManagedDSMap(newObject));
    }

    public ManagedDatastreamComparator(DigitalObject old, DigitalObject current) {
        compare(getManagedDSMap(old), getManagedDSMap(current));
    }

    public Collection<Datastream> getAdded() {
        return m_added;
    }

    public Collection<Datastream> getDeleted() {
        return m_removed;
    }

    private void compare(Map<String, Datastream> old,
                         Map<String, Datastream> current) {

        /* First, get new versions from current */
        for (Map.Entry<String, Datastream> e : current.entrySet()) {
            if (!old.containsKey(e.getKey())) {
                m_added.add(e.getValue());
            }
        }

        /* Now, get purged versions from old */
        for (Map.Entry<String, Datastream> e : old.entrySet()) {
            if (!current.containsKey(e.getKey())) {
               m_removed.add(e.getValue());
            }
        }
    }

    private Map<String, Datastream> getManagedDSMap(DigitalObject obj) {
        Map<String, Datastream> map = new HashMap<String, Datastream>();

        Iterator<String> ids = obj.datastreamIdIterator();

        while (ids.hasNext()) {
            String dsID = ids.next();
            for (Datastream d : obj.datastreams(dsID)) {
                if ("M".equals(d.DSControlGrp)) {
                    map.put(d.DSVersionID, d);
                }
            }
        }

        return map;
    }
}