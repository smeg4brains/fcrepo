
package org.fcrepo.server.storage.highlevel.hbase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.fcrepo.server.storage.highlevel.OutOfDateException;

import org.fcrepo.server.errors.LowlevelStorageException;

/** Performs atomic writes, or fakes them if necessary */
public class SafeWrite {

    private static final byte[] META_FAMILY = "meta".getBytes();

    private static final byte[] TS_COLUMN = "ts".getBytes();

    private static final byte[] FOLLOWUP_ACTION_COLUMN =
            "action.1.delete".getBytes();

    private final String m_pid;

    private final HTable m_table;

    private final byte[] m_old_ts;

    private final byte[] m_new_ts;

    private Action m_first_action;

    private final LinkedHashMap<ExpectedColumnValue, Action> m_actions =
            new LinkedHashMap<ExpectedColumnValue, Action>();

    public SafeWrite(String pid, byte[] old_ts, byte[] new_ts, HTable table) {
        m_new_ts = new_ts;
        m_old_ts = old_ts;
        m_pid = pid;
        m_table = table;
    }

    public static void finish(byte[] row, HTable table) {

    }

    /**
     * Add a delete step. Deletes are expected to be the SECOND step in a write,
     * if that write involves deletes. The first step nulls the FOXML and zeroes
     * out the datastreams, and the second delete step actually purges the
     * necessary datastreams
     */
    public void addStep(Delete delete) {
        if (m_actions.size() == 1) {

            delete.deleteColumn(META_FAMILY, FOLLOWUP_ACTION_COLUMN);

            byte[] serializedAction = serializeDeleteAction(delete);

            m_first_action.getPutCall().add(META_FAMILY,
                                            FOLLOWUP_ACTION_COLUMN,
                                            serializedAction);

            m_actions.put(new ExpectedColumnValue(META_FAMILY,
                                                  FOLLOWUP_ACTION_COLUMN,
                                                  serializedAction),
                          new Action(delete));
        } else {
            throw new IllegalStateException("DELETES are only allowed as a second step");
        }
    }

    /**
     * Add a put step.
     * <p>
     * In reality, there may be only one PUT step per write, and it must be the
     * first action. This first PUT is, at a minimum, expected to modify the
     * FOXML
     * </p>
     */
    public void addStep(Put put) {
        if (m_actions.size() == 0) {
            m_first_action = new Action(put);
            m_actions.put(new ExpectedColumnValue(META_FAMILY,
                                                  TS_COLUMN,
                                                  m_old_ts), m_first_action);
        } else {
            throw new IllegalStateException("PUTs are only allowed as a first step!");
        }
    }

    public void execute() throws LowlevelStorageException {

        /*
         * If we have two actions, corrupt the timestamp in the first step to
         * disallow other processes from performing modifications between the
         * two steps. Then and add an additional step at the end to restore the
         * timestamp, completing the process
         */
        if (m_actions.size() == 2) {

            /* Modify the first step so that it corrupts the timestamp */
            m_first_action.getPutCall().add(META_FAMILY,
                                            TS_COLUMN,
                                            getNegativeValue(m_new_ts));

            /* Add third step to restore timestamp */
            Put putBackTS = new Put(m_pid.getBytes());
            putBackTS.add(META_FAMILY, TS_COLUMN, m_new_ts);

            m_actions.put(new ExpectedColumnValue(META_FAMILY,
                                                  TS_COLUMN,
                                                  getNegativeValue(m_new_ts)),
                          new Action(putBackTS));
        } else {
            m_first_action.getPutCall().add(META_FAMILY, TS_COLUMN, m_new_ts);
        }

        /* Now, perform all actions */
        for (Map.Entry<ExpectedColumnValue, Action> step : m_actions.entrySet()) {
            Action action = step.getValue();
            ExpectedColumnValue expected = step.getKey();
            if (!action.exec(expected)) {
                Get get = new Get(action.getRow());
                get.addColumn(expected.family, expected.column);
                System.err.println("EXPECTED FAMILY: "
                        + new String(expected.family));
                System.err.println("EXPECTED COLUMN: "
                        + new String(expected.column));
                System.err.println("EXPECTED VALUE:"
                        + new String(expected.value));
                try {
                    System.err.println("ACTUAL VALUE: "
                            + new String(m_table.get(get).value()));
                } catch (Exception e) {

                }
                throw new OutOfDateException(false, "Object " + m_pid
                        + " has been modified by another writer");
            }
        }
    }

    private byte[] serializeDeleteAction(Delete delete) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            delete.write(new DataOutputStream(out));
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize delete action", e);
        }
    }

    private byte[] getNegativeValue(byte[] ts) {
        byte[] newTs = new byte[ts.length + 1];
        newTs[0] = '-';
        for (int i = 0; i < ts.length; i++) {
            newTs[i + 1] = ts[i];
        }
        return newTs;
    }

    private class Action {

        private Put m_put;

        private Delete m_delete;

        public Action(Delete delete) {
            m_delete = delete;
        }

        public Action(Put put) {
            m_put = put;
        }

        public Put getPutCall() {
            return m_put;
        }

        public boolean exec(ExpectedColumnValue expected_val)
                throws LowlevelStorageException {
            try {
                if (m_put != null) {
                    return m_table.checkAndPut(m_put.getRow(),
                                               expected_val.family,
                                               expected_val.column,
                                               expected_val.value,
                                               m_put);
                } else {
                    m_table.delete(m_delete);
                    return true;
                }
            } catch (IOException e) {
                throw new LowlevelStorageException(true,
                                                   "Could not write to HBase",
                                                   e);
            }
        }

        public byte[] getRow() {
            if (m_put != null) {
                return m_put.getRow();
            } else {
                return m_delete.getRow();
            }
        }
    }

    private class ExpectedColumnValue {

        public final byte[] family;

        public final byte[] column;

        public final byte[] value;

        public ExpectedColumnValue(byte[] family,
                                   byte[] column,
                                   byte[] expectedvalue) {
            this.family = family;
            this.column = column;
            this.value = expectedvalue;
        }
    }

}
