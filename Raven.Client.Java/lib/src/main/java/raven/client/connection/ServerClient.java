package raven.client.connection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;

import com.google.common.base.Function;

import raven.abstractions.data.Attachment;
import raven.client.data.Constants;
import raven.client.document.DocumentConvention;
import raven.client.extensions.MultiDatabase;
import raven.client.json.JsonDocument;
import raven.client.json.PutResult;
import raven.client.json.RavenJArray;
import raven.client.json.RavenJObject;
import raven.client.json.RavenJToken;
import raven.client.json.RavenJValue;
import raven.client.json.lang.ConcurrencyException;
import raven.client.json.lang.HttpOperationException;
import raven.client.json.lang.ServerClientException;
import raven.client.utils.StringUtils;
import raven.client.utils.UrlUtils;


public class ServerClient implements IDatabaseCommands {

  private String url;
  private HttpClient httpClient;
  private final HttpJsonRequestFactory jsonRequestFactory;

  private final DocumentConvention convention;
  private final Credentials credentials;
  private final Map<String, String> operationsHeaders = new HashMap<>();

  public ServerClient(String url) {
    super();
    if (url.endsWith("/")) {
      url = url.substring(0, url.length() - 1);
    }

    this.url = url;
    httpClient = new HttpClient();
    jsonRequestFactory = new HttpJsonRequestFactory();
    convention = new DocumentConvention(); //TODO: update me
    credentials = new Credentials();//TODO: update me
    //TODO: operations headers
  }

  protected void addTransactionInformation(RavenJObject metadata) {
    // TODO implemenent me!

  }

  @Override
  public void delete(final String key, final UUID etag) throws ServerClientException {
    ensureIsNotNullOrEmpty(key, "key");
    executeWithReplication(HttpMethods.DELETE, new Function<String, Void>() {
      @Override
      public Void apply(String u) {
        directDelete(u, key, etag);
        return null;
      }
    });
  }

  @Override
  public void deleteAttachment(final String key, final UUID etag) {
    executeWithReplication(HttpMethods.DELETE, new Function<String, Void>() {
      @Override
      public Void apply(String operationUrl) {
        directDeleteAttachment(key, etag, operationUrl);
        return null;
      }
    });
  }

  //TODO: review me
  private void directDelete(String serverUrl, String key, UUID etag) throws ServerClientException{
    RavenJObject metadata = new RavenJObject();
    if (etag != null) {
      metadata.add("ETag", RavenJToken.fromObject(etag.toString()));
    }
    try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, serverUrl + "/docs/" + UrlUtils.escapeDataString(key), HttpMethods.DELETE, metadata, credentials, convention).
        addOperationHeaders(operationsHeaders))) {
      //TODO: AddReplicationStatusHeaders(Url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
      jsonRequest.executeRequest();
    } catch (HttpOperationException e) {
      if (HttpStatus.SC_CONFLICT == e.getStatusCode()) {
        throwConcurrencyException(e);
      } else {
        throw e;
      }
    } catch (Exception e) {
      throw new ServerClientException(e);
    }

  }

  protected void directDeleteAttachment(String key, UUID etag, String operationUrl) {
    RavenJObject metadata = new RavenJObject();
    if (etag != null) {
      metadata.add("ETag", RavenJToken.fromObject(etag.toString()));
    }
    try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + UrlUtils.escapeDataString(key), HttpMethods.DELETE, metadata, credentials, convention))) {
      //TODO: AddReplicationStatusHeaders(Url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges
      jsonRequest.executeRequest();
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  /**
   * Perform a direct get for a document with the specified key on the specified server URL.
   * @param serverUrl
   * @param key
   * @return
   */
  //TODO: review me
  private JsonDocument directGet(String serverUrl, String key) throws ServerClientException {
    RavenJObject metadata = new RavenJObject();
    addTransactionInformation(metadata);
    try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, serverUrl + "/docs/" + UrlUtils.escapeDataString(key), HttpMethods.GET, metadata, credentials, convention).
        addOperationHeaders(operationsHeaders))) {
      //TODO: AddReplicationStatusHeaders
      RavenJToken responseJson = jsonRequest.getResponseAsJson(HttpStatus.SC_OK);

      String docKey = jsonRequest.getResponseHeader(Constants.DOCUMENT_ID_FIELD_NAME);
      if (docKey == null) {
        docKey = key;
      }
      docKey = UrlUtils.unescapeDataString(docKey);

      return SerializationHelper.deserializeJsonDocument(docKey, responseJson, jsonRequest);

    } catch (HttpOperationException e) {
      HttpOperationException httpException = (HttpOperationException) e;
      if (httpException.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        return null;
      } else if (httpException.getStatusCode() == HttpStatus.SC_CONFLICT) {
        //TODO: resolve conflicts
      }
      throw new ServerClientException(e);
    } catch (Exception e) {
        throw new ServerClientException(e);
    }
  }
  protected Attachment directGetAttachment(HttpMethods method, String key, String operationUrl) {

    try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + UrlUtils.escapeDataString(key), method, new RavenJObject(), credentials, convention).
        addOperationHeaders(operationsHeaders))) {
      //TODO: AddReplicationStatusHeaders
      jsonRequest.executeRequest();

      if (HttpMethods.GET == method) {
        byte[] responseBytes = jsonRequest.getResponseBytes();
        return new Attachment(responseBytes, responseBytes.length, jsonRequest.filterHeadersAttachment(), jsonRequest.getEtagHeader(), key);
      } else {
        return new Attachment(null, Integer.valueOf(jsonRequest.getResponseHeader("Content-Length")), jsonRequest.filterHeadersAttachment(), jsonRequest.getEtagHeader(), key);
      }

      //TODO: HandleReplicationStatusChanges(webRequest.ResponseHeaders, Url, operationUrl);

    } catch (Exception e) {
      //TODO: resolve conflicts

      throw new ServerClientException(e);
    }

  }


  protected List<Attachment> directGetAttachmentHeadersStartingWith(HttpMethods method, String idPrefix, int start, int pageSize, String operationUrl) {
    try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/?startsWith" + idPrefix + "&start=" + start + "&pageSize=" + pageSize, HttpMethods.GET, new RavenJObject(), credentials, convention))) {
      //TODO: AddReplicationStatusHeaders
      RavenJToken responseJson = jsonRequest.getResponseAsJson(HttpStatus.SC_OK);

      return SerializationHelper.deserializeAttachements(responseJson);

    } catch (Exception e) {
        throw new ServerClientException(e);
    }
  }

  private PutResult directPut(RavenJObject metadata, String key, UUID etag, RavenJObject document, String operationUrl) throws ServerClientException {
    if (metadata == null) {
      metadata = new RavenJObject();
    }
    HttpMethods method = StringUtils.isNotNullOrEmpty(key) ? HttpMethods.PUT : HttpMethods.POST;
    if (etag != null) {
      metadata.set("ETag", new RavenJValue(etag.toString()));
    }
    if (key != null) {
      key = UrlUtils.escapeDataString(key);
    }

    String requestUrl = operationUrl + "/docs/" + ((key != null) ? key : "");

    try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUrl, method, metadata, credentials, convention).
        addOperationHeaders(operationsHeaders))) {
      //TODO: .AddReplicationStatusHeaders(Url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
      jsonRequest.write(document.toString());
      RavenJObject responseJson = (RavenJObject) jsonRequest.getResponseAsJson(HttpStatus.SC_CREATED);

      return new PutResult(responseJson.value(String.class, "Key"), responseJson.value(UUID.class, "ETag"));
    } catch (Exception e) {
      if (e instanceof HttpOperationException) {
        HttpOperationException httpException = (HttpOperationException)e;
        if (httpException.getStatusCode() == HttpStatus.SC_CONFLICT) {
          throwConcurrencyException(e);
        }
      }
      throw new ServerClientException(e);
    }
  }

  protected void directPutAttachment(String key, RavenJObject metadata, UUID etag, InputStream data, String operationUrl) {
    if (etag != null) {
      metadata.set("Etag", new RavenJValue(etag.toString()));
    }

    try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + UrlUtils.escapeDataString(key), HttpMethods.PUT, metadata, credentials, convention))) {
      //TODO: .AddReplicationStatusHeaders(Url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
      jsonRequest.write(data);
      jsonRequest.executeRequest();
    } catch (HttpOperationException e) {
      if (e.getStatusCode() != HttpStatus.SC_INTERNAL_SERVER_ERROR) {
        throw e;
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        IOUtils.copy(e.getMethodBase().getResponseBodyAsStream(), baos);
      } catch (IOException e1) {
        throw new ServerClientException(e1);
      }
      throw new ServerClientException("Internal server error: " + baos.toString());
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  //TODO: public bool InFailoverMode()

  private List<JsonDocument> directStartsWith(String serverUrl, String keyPrefix, String matches, int start, int pageSize, boolean metadataOnly) throws ServerClientException {
    String actualUrl = serverUrl + String.format("/docs?startsWith=%s&matches=%s&start=%d&pageSize=%d", UrlUtils.escapeDataString(keyPrefix),
        StringUtils.defaultIfNull(matches, ""), start, pageSize);
    if (metadataOnly) {
      actualUrl += "&metadata-only=true";
    }
    RavenJObject metadata = new RavenJObject();

    try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, actualUrl, HttpMethods.GET,  metadata, credentials, convention).
        addOperationHeaders(operationsHeaders))) {
      //TODO: AddReplicationStatusHeaders(Url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
      RavenJToken responseJson = jsonRequest.getResponseAsJson(HttpStatus.SC_OK);
      return SerializationHelper.ravenJObjectsToJsonDocuments(responseJson);
    } catch (Exception e) {
      //TODO: resolve conflicts
      throw new ServerClientException(e);
    }
  }

  protected void directUpdateAttachmentMetadata(String key, RavenJObject metadata, UUID etag, String operationUrl) {
    if (etag != null) {
      metadata.set("Etag", new RavenJValue(etag.toString()));
    }

    try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + UrlUtils.escapeDataString(key), HttpMethods.POST, metadata, credentials, convention))) {
      //TODO: .AddReplicationStatusHeaders(Url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
      jsonRequest.executeRequest();
    } catch (HttpOperationException e) {
      if (e.getStatusCode() != HttpStatus.SC_INTERNAL_SERVER_ERROR) {
        throw e;
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        IOUtils.copy(e.getMethodBase().getResponseBodyAsStream(), baos);
      } catch (IOException e1) {
        throw new ServerClientException(e1);
      }
      throw new ServerClientException("Internal server error: " + baos.toString());
    } catch (Exception e) {
      throw new ServerClientException(e);
    }

  }

  private void ensureIsNotNullOrEmpty(String key, String argName) {
    if (key == null || "".equals(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty " + argName);
    }
  }

  public RavenJToken executeGetRequest(final String requestUrl) {
    ensureIsNotNullOrEmpty(requestUrl, "url");
    return executeWithReplication(HttpMethods.GET, new Function<String, RavenJToken>() {

      @Override
      public RavenJToken apply(String serverUrl) {
        RavenJObject metadata = new RavenJObject();
        addTransactionInformation(metadata);
        try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
            new CreateHttpJsonRequestParams(ServerClient.this, serverUrl + requestUrl, HttpMethods.GET, metadata, credentials, convention).
            addOperationHeaders(operationsHeaders)
            )) {
          return jsonRequest.getResponseAsJson(HttpStatus.SC_OK);
        } catch (Exception e) {
          throw new ServerClientException(e.getMessage(), e);
        }
      }
    });
  }
  //TODO: public HttpJsonRequest CreateRequest(string method, string requestUrl, bool disableRequestCompression = false)

  //TODO: public IndexDefinition[] GetIndexes(int start, int pageSize)
  //TODO: public void ResetIndex(string name)
  //TODO: private object DirectResetIndex(string name, string operationUrl)
  //TODO: private string[] DirectGetIndexNames(int start, int pageSize, string operationUrl)
  //TODO: public IndexDefinition GetIndex(string name)
  //TODO: private IndexDefinition DirectGetIndex(string indexName, string operationUrl)

  private <T> T executeWithReplication(HttpMethods method, Function<String, T> operation) throws ServerClientException {
    //TODO: implement me !
    return operation.apply(url);
  }

  public IDatabaseCommands forDatabase(String database) {
    String databaseUrl = MultiDatabase.getRootDatabaseUrl(url);
    databaseUrl = url + "/databases/" + database;
    if (databaseUrl.equals(url)) {
      return this;
    }
    return new ServerClient(databaseUrl);
  }

  public IDatabaseCommands forSystemDatabase() {
    String databaseUrl = MultiDatabase.getRootDatabaseUrl(url);
    if (databaseUrl.equals(url)) {
      return this;
    }
    return new ServerClient(databaseUrl);
  }

  @Override
  public JsonDocument get(final String key) throws ServerClientException {
    ensureIsNotNullOrEmpty(key, "key");
    return executeWithReplication(HttpMethods.GET, new Function<String, JsonDocument>() {
      @Override
      public JsonDocument apply(String u) {
        return directGet(u, key);
      }
    });
  }

  @Override
  public Attachment getAttachment(final String key) {
    return executeWithReplication(HttpMethods.GET, new Function<String, Attachment>() {
      @Override
      public Attachment apply(String operationUrl) {
        return directGetAttachment(HttpMethods.GET, key, operationUrl);
      }
    });
  }

  public List<Attachment> getAttachmentHeadersStartingWith(final String idPrefix, final int start, final int pageSize) {
    return executeWithReplication(HttpMethods.GET, new Function<String, List<Attachment>>() {

      @Override
      public List<Attachment> apply(String operationUrl) {
        return directGetAttachmentHeadersStartingWith(HttpMethods.GET, idPrefix, start, pageSize, operationUrl);
      }
    });
  }

  //TODO: private void HandleReplicationStatusChanges(NameValueCollection headers, string primaryUrl, string currentUrl)

  //TODO: private ConflictException TryResolveConflictOrCreateConcurrencyException(string key, RavenJObject conflictsDoc, Guid etag)

  public List<String> getDatabaseNames(int pageSize, int start) {
    String url = RavenUrlExtensions.databases("", pageSize, start);
    url = RavenUrlExtensions.noCache(url);
    RavenJArray json = (RavenJArray) executeGetRequest(url);
    List<String> result = new ArrayList<>();
    for (RavenJToken token: json) {
      RavenJValue value = (RavenJValue) token ;
      result.add((String)value.getValue());
    }
    return result;
  }

  @Override
  public List<JsonDocument> getDocuments(final int start, final int pageSize, final boolean metadataOnly) {
    return executeWithReplication(HttpMethods.GET, new Function<String, List<JsonDocument>>() {
      @Override
      public List<JsonDocument> apply(String url) {
        String requestUri = url + "/docs?start=" + start + "&pageSize=" + pageSize;
        if (metadataOnly) {
          requestUri += "&metadata-only=true";
        }
        try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
            new CreateHttpJsonRequestParams(ServerClient.this, RavenUrlExtensions.noCache(requestUri), HttpMethods.GET,  new RavenJObject(), credentials, convention).
            addOperationHeaders(operationsHeaders))) {

          RavenJToken responseJson = jsonRequest.getResponseAsJson(HttpStatus.SC_OK);
          return SerializationHelper.ravenJObjectsToJsonDocuments(responseJson);
        } catch (Exception e) {
          throw new ServerClientException(e);
        }
      }
    });
  }

  /**
   * @return the httpClient
   */
  protected HttpClient getHttpClient() {
    return httpClient;
  }

  /**
   * @return the url
   */
  public String getUrl() {
    return url;
  }

  @Override
  public Attachment headAttachment(final String key) {
    return executeWithReplication(HttpMethods.HEAD, new Function<String, Attachment>() {
      @Override
      public Attachment apply(String operationUrl) {
        return directGetAttachment(HttpMethods.HEAD, key, operationUrl);
      }
    });
  }

  @Override
  public PutResult put(final String key, final UUID etag, final RavenJObject document, final RavenJObject metadata) throws ServerClientException {
    return executeWithReplication(HttpMethods.PUT, new Function<String, PutResult>() {
      @Override
      public PutResult apply(String u) {
        return directPut(metadata, key,  etag, document, u);
      }
    });
  }

  @Override
  public void putAttachment(final String key, final UUID etag, final InputStream data, final RavenJObject metadata) {
    executeWithReplication(HttpMethods.PUT, new Function<String, Void>() {
      @Override
      public Void apply(String operationUrl) {
        directPutAttachment(key, metadata, etag, data, operationUrl);
        return null;
      }
    });
  }

  @Override
  public List<JsonDocument> startsWith(final String keyPrefix, final String matches, final int start, final int pageSize, final boolean metadataOnly) throws ServerClientException {
    ensureIsNotNullOrEmpty(keyPrefix, "keyPrefix");
    return executeWithReplication(HttpMethods.GET, new Function<String, List<JsonDocument>>() {
      @Override
      public List<JsonDocument> apply(String u) {
        return directStartsWith(u, keyPrefix, matches, start, pageSize, metadataOnly);
      }
    });
  }

  //TODO :public string PutIndex(string name, IndexDefinition definition)
  //TODO: public string DirectPutIndex(string name, string operationUrl, bool overwrite, IndexDefinition definition)
  //TODO: public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef)
  //TODO: public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, bool overwrite)
  //TODO: public QueryResult Query(string index, IndexQuery query, string[] includes, bool metadataOnly = false, bool indexEntriesOnly = false)
  //TODO: private QueryResult DirectQuery(string index, IndexQuery query, string operationUrl, string[] includes, bool metadataOnly, bool includeEntries)
  //TODO: public void DeleteIndex(string name)
  //TODO: private void DirectDeleteIndex(string name, string operationUrl)

  private ConcurrencyException throwConcurrencyException(Exception e) {
    UUID expectedEtag  = null;
    UUID actualEtag = null;
    //TODO: implement me!
    return new ConcurrencyException(expectedEtag, actualEtag, e);
  }

  @Override
  public void updateAttachmentMetadata(final String key, final UUID etag, final RavenJObject metadata) {
    executeWithReplication(HttpMethods.POST, new Function<String, Void>() {
      @Override
      public Void apply(String operationUrl) {
        directUpdateAttachmentMetadata(key, metadata, etag, operationUrl);
        return null;
      }
    });
  }


}
