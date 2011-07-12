package org.fcrepo.server.storage.highlevel.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import jena.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.fcrepo.server.Module;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.GeneralException;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.errors.UnrecognizedFieldException;
import org.fcrepo.server.search.Condition;
import org.fcrepo.server.search.EmptyResult;
import org.fcrepo.server.search.FieldSearch;
import org.fcrepo.server.search.FieldSearchQuery;
import org.fcrepo.server.search.FieldSearchResult;
import org.fcrepo.server.search.ObjectFields;
import org.fcrepo.server.storage.DOReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseFieldSearch extends Module implements FieldSearch {
	private static final Logger log = LoggerFactory.getLogger(HBaseFieldSearch.class);
	private final HTable objectTable;
	private final HadoopHighLevelStorageProperties properties;

	public HBaseFieldSearch(Map<String, String> moduleParameters, Server server, String role, HadoopHighLevelStorageProperties props)
			throws ModuleInitializationException {
		super(moduleParameters, server, role);
		this.properties = props;
		try {
			objectTable = new HTable(props.getObjectTableNameAsBytes());
		} catch (IOException e) {
			log.error("Unable to connect to HTable " + props.getObjectTableName());
			throw new ModuleInitializationException("unable to connect to HTable reource " + props.getObjectTableName() + ": " + e.getLocalizedMessage(), role);
		}
	}

	@Override
	public void update(DOReader reader) throws ServerException {
		// HM?
		log.debug("updating reader for PID " + reader.GetObjectPID());
	}

	@Override
	public boolean delete(String pid) throws ServerException {
		// WUS?!
		log.debug("deleting pid from fieldsearch " + pid);
		return false;
	}

	@Override
	public FieldSearchResult findObjects(String[] resultFields, int maxResults, FieldSearchQuery query) throws ServerException {
		try {
			if (query.getType() == FieldSearchQuery.TERMS_TYPE) {
				log.debug("searching for terms " + query.getTerms());
				Scan s = new Scan();
				Iterator<Result> objects = objectTable.getScanner(s).iterator();
				HBaseFieldSearchResult result = new HBaseFieldSearchResult(resultFields,objects,properties);
				return result;
			} else {
				throw new UnsupportedOperationException("not yet implemented");
			}
		} catch (Exception e) {
			log.error("unable to search for objects", e);
			throw new GeneralException(e.getLocalizedMessage(), e);
		}
	}

	@Override
	public FieldSearchResult resumeFindObjects(String sessionToken) throws ServerException {
		log.debug("resuming search for token " + sessionToken);
		return new EmptyResult();
	}

	public class HBaseFieldSearchResult implements FieldSearchResult {
		private List<ObjectFields> objectFields = new ArrayList<ObjectFields>();
		private Date expirationDate;
		private String token;
		private long cursor;

		public HBaseFieldSearchResult(String[] resultFields,Iterator<Result> hbaseResults,HadoopHighLevelStorageProperties props) {
			expirationDate = new Date();
			token = UUID.randomUUID().toString();
			while (hbaseResults.hasNext()) {
				Result res = hbaseResults.next();
				log.debug("adding " + new String(res.getRow(),HadoopHighLevelStorageProperties.getCharset()));
				ObjectFields f;
				try {
					String[] fields={"pid"};
					f = new ObjectFields(fields);
				} catch (UnrecognizedFieldException e) {
					log.error("unable to create search results");
					throw new RuntimeException("unable to create search result ",e);
				}
				f.setPid(new String(res.getRow(),HadoopHighLevelStorageProperties.getCharset()));
				objectFields.add(f);
			}
		}

		@Override
		public List<ObjectFields> objectFieldsList() {
			return objectFields;
		}

		@Override
		public String getToken() {
			return token;
		}

		@Override
		public long getCursor() {
			return cursor;
		}

		@Override
		public long getCompleteListSize() {
			return objectFields.size();
		}

		@Override
		public Date getExpirationDate() {
			return expirationDate;
		}

	}

}
