package org.fcrepo.server.search;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.fcrepo.server.search.FieldSearchResult;
import org.fcrepo.server.search.ObjectFields;

public class EmptyResult
        implements FieldSearchResult {

    public long getCompleteListSize() {
        return 0;
    }

    public long getCursor() {
        return 0;
    }

    public Date getExpirationDate() {
        return new Date(0);
    }

    public String getToken() {
        return "-";
    }

    public List<ObjectFields> objectFieldsList() {
        return new ArrayList<ObjectFields>();
    }
}