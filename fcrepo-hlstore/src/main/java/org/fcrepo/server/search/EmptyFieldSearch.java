package org.fcrepo.server.search;

import java.util.Map;

import org.fcrepo.server.Module;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.search.FieldSearch;
import org.fcrepo.server.search.FieldSearchQuery;
import org.fcrepo.server.search.FieldSearchResult;
import org.fcrepo.server.storage.DOReader;

/** Field Search impl that does nothing */
public class EmptyFieldSearch extends Module
        implements FieldSearch {

    public EmptyFieldSearch(Map<String, String> moduleParameters,
                            Server server,
                            String role)
            throws ModuleInitializationException {
        super(moduleParameters, server, role);
    }

    public void update(DOReader reader) throws ServerException {
       return;
    }

    public boolean delete(String pid) throws ServerException {
       return true;
    }

    public FieldSearchResult findObjects(String[] resultFields,
                                         int maxResults,
                                         FieldSearchQuery query)
            throws ServerException {
        return new EmptyResult();
    }

    public FieldSearchResult resumeFindObjects(String sessionToken)
            throws ServerException {
        return new EmptyResult();
    }

}
