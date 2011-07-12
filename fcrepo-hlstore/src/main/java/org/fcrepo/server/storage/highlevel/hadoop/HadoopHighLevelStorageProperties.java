package org.fcrepo.server.storage.highlevel.hadoop;

import java.net.URI;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;

public class HadoopHighLevelStorageProperties {
	private Configuration configuration;
	private String objectTableName;
	private String contentColumnName;
	private String datastreamTableName;
	private Charset charset = Charset.forName("UTF-8");
	private String defaultQualifier;
	private String defaultFormat;
	private String defaultEncoding;
	private URI uri;
	private String username;
	private String passwd;

	public String getDefaultQualifier() {
		return defaultQualifier;
	}
	
	public byte[] getDefaultQualifierAsBytes() {
		return defaultQualifier.getBytes(charset);
	}

	public void setDefaultQualifier(String defaultQualifier) {
		this.defaultQualifier = defaultQualifier;
	}

	public String getDefaultFormat() {
		return defaultFormat;
	}

	public byte[] getDefaultFormatAsBytes() {
		return defaultFormat.getBytes(charset);
	}

	public void setDefaultFormat(String defaultFormat) {
		this.defaultFormat = defaultFormat;
	}

	public String getDefaultEncoding() {
		return defaultEncoding;
	}

	public byte[] getDefaultEncodingAsBytes() {
		return defaultEncoding.getBytes(charset);
	}

	public void setDefaultEncoding(String defaultEncoding) {
		this.defaultEncoding = defaultEncoding;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public String getContentColumnName() {
		return contentColumnName;
	}
	public byte[] getContentColumnNameAsBytes() {
		return contentColumnName.getBytes(charset);
	}

	public void setContentColumnName(String contentColumnName) {
		this.contentColumnName = contentColumnName;
	}

	public String getObjectTableName() {
		return objectTableName;
	}

	public byte[] getObjectTableNameAsBytes() {
		return objectTableName.getBytes(charset);
	}

	public void setObjectTableName(String objectTableName) {
		this.objectTableName = objectTableName;
	}

	public String getDatastreamTableName() {
		return datastreamTableName;
	}

	public byte[] getDatastreamTableNameAsBytes() {
		return datastreamTableName.getBytes(charset);
	}

	public void setDatastreamTableName(String datastreamTableName) {
		this.datastreamTableName = datastreamTableName;
	}

	public Charset getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = Charset.forName(charset);
	}

	public URI getUri() {
		return uri;
	}

	public void setUri(URI uri) {
		this.uri = uri;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPasswd() {
		return passwd;
	}

	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}

}
