package raven.client.connection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;

import raven.abstractions.closure.Action3;
import raven.abstractions.closure.Function1;
import raven.abstractions.data.Attachment;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.JsonDocumentMetadata;
import raven.abstractions.data.MultiLoadResult;
import raven.abstractions.data.Constants;
import raven.abstractions.data.PutResult;
import raven.abstractions.exceptions.ConcurrencyException;
import raven.abstractions.exceptions.HttpOperationException;
import raven.abstractions.exceptions.ServerClientException;
import raven.abstractions.extensions.MetadataExtensions;
import raven.abstractions.json.linq.RavenJArray;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.abstractions.json.linq.RavenJValue;
import raven.client.document.DocumentConvention;
import raven.client.extensions.MultiDatabase;
import raven.client.listeners.IDocumentConflictListener;
import raven.client.utils.UrlUtils;

//TODO: finish me
public class ServerClient implements IDatabaseCommands {

  private String url;
  private final HttpJsonRequestFactory jsonRequestFactory;

  private final DocumentConvention convention;
  private final Credentials credentials;
  private final ReplicationInformer replicationInformer;
  private final Map<String, String> operationsHeaders = new HashMap<>();
  private final UUID currentSessionId;
  private final IDocumentConflictListener[] conflictListeners;

  private int readStripingBase;

  public ServerClient(String url, DocumentConvention convention, Credentials credentials, ReplicationInformer replicationInformer, HttpJsonRequestFactory httpJsonRequestFactory,
      UUID currentSessionId, IDocumentConflictListener[] conflictListeners) {
    //TODO: profiling information
    this.credentials = credentials;
    this.replicationInformer = replicationInformer;
    this.jsonRequestFactory = httpJsonRequestFactory;
    this.currentSessionId = currentSessionId;
    this.conflictListeners = conflictListeners;
    this.url = url;

    if (url.endsWith("/")) {
      this.url = url.substring(0, url.length() - 1);
    }
    this.convention = convention;

    replicationInformer.updateReplicationInformationIfNeeded(this);
    this.readStripingBase = replicationInformer.getReadStripingBase();

  }

  /**
   * @return the replicationInformer
   */
  public ReplicationInformer getReplicationInformer() {
    return replicationInformer;
  }

  /**
   * @return the operationsHeaders
   */
  public Map<String, String> getOperationsHeaders() {
    return operationsHeaders;
  }

  protected void addTransactionInformation(RavenJObject metadata) {

    // TODO implemenent me!

  }

  @Override
  public void delete(final String key, final UUID etag) throws ServerClientException {
    ensureIsNotNullOrEmpty(key, "key");
    executeWithReplication(HttpMethods.DELETE, new Function1<String, Void>() {
      @Override
      public Void apply(String u) {
        directDelete(u, key, etag);
        return null;
      }
    });
  }

  @Override
  public void deleteAttachment(final String key, final UUID etag) {
    executeWithReplication(HttpMethods.DELETE, new Function1<String, Void>() {
      @Override
      public Void apply(String operationUrl) {
        directDeleteAttachment(key, etag, operationUrl);
        return null;
      }
    });
  }

  private void directDelete(String serverUrl, String key, UUID etag) throws ServerClientException {
    RavenJObject metadata = new RavenJObject();
    if (etag != null) {
      metadata.add("ETag", RavenJToken.fromObject(etag.toString()));
    }
    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, serverUrl + "/docs/" + UrlUtils.escapeDataString(key), HttpMethods.DELETE, metadata, credentials, convention).addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, serverUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());
    try {
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
    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + UrlUtils.escapeDataString(key), HttpMethods.DELETE, metadata, credentials, convention)).addReplicationStatusHeaders(url,
        operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());
    try {
      jsonRequest.executeRequest();
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  protected class HandleReplicationStatusChangesCallback implements Action3<Map<String, String>, String, String> {
    @Override
    public void apply(Map<String, String> headers, String primaryUrl, String currentUrl) {
      handleReplicationStatusChanges(headers, primaryUrl, currentUrl);
    }
  }

  private void handleReplicationStatusChanges(Map<String, String> headers, String primaryUrl, String currentUrl) {
    //TODO: finish me
  }

  /**
   * Perform a direct get for a document with the specified key on the specified server URL.
   * @param serverUrl
   * @param key
   * @return
   */
  private JsonDocument directGet(String serverUrl, String key) throws ServerClientException {
    RavenJObject metadata = new RavenJObject();
    addTransactionInformation(metadata);

    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, serverUrl + "/docs/" + UrlUtils.escapeDataString(key), HttpMethods.GET, metadata, credentials, convention).addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, serverUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());
    try {
      RavenJToken responseJson = jsonRequest.readResponseJson();

      String docKey = jsonRequest.getResponseHeaders().get(Constants.DOCUMENT_ID_FIELD_NAME);
      if (docKey == null) {
        docKey = key;
      }
      docKey = UrlUtils.unescapeDataString(docKey);
      //TODO: don't include document id in metadata
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

  protected MultiLoadResult directGet(String[] ids, String u, String[] includes, boolean metadataOnly) {
    // TODO Auto-generated method stub
    throw new IllegalStateException("not implemeneted yet!");
  }

  protected Attachment directGetAttachment(HttpMethods method, String key, String operationUrl) {
    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + UrlUtils.escapeDataString(key), method, new RavenJObject(), credentials, convention).addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());
    try {

      jsonRequest.executeRequest();

      if (HttpMethods.GET == method) {
        byte[] responseBytes = jsonRequest.readResponseBytes();
        return new Attachment(true, responseBytes, responseBytes.length,
            MetadataExtensions.filterHeadersAttachment(jsonRequest.getResponseHeaders()),
            HttpExtensions.getEtagHeader(jsonRequest), key);
      } else {
        return new Attachment(false, null, Integer.valueOf(jsonRequest.getResponseHeaders().get("Content-Length")),
            MetadataExtensions.filterHeadersAttachment(jsonRequest.getResponseHeaders()), HttpExtensions.getEtagHeader(jsonRequest), key);
      }

      //TODO: HandleReplicationStatusChanges(webRequest.ResponseHeaders, Url, operationUrl);

    } catch (HttpOperationException e) {
      if (e.getStatusCode() == HttpStatus.SC_CONFLICT) {
        //TODO: handle conflicts
      } else if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        return null;
      }
      throw new ServerClientException(e);

    } catch (Exception e) {
      throw new ServerClientException(e);
    }

  }

  protected List<Attachment> directGetAttachmentHeadersStartingWith(HttpMethods method, String idPrefix, int start, int pageSize, String operationUrl) {
    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/?startsWith" + idPrefix + "&start=" + start + "&pageSize=" + pageSize, HttpMethods.GET, new RavenJObject(), credentials,
            convention)).addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());
    try {
      RavenJToken responseJson = jsonRequest.readResponseJson();

      return SerializationHelper.deserializeAttachements(responseJson);

    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  protected Long directNextIdentityFor(String name, String operationUrl) {
    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl + "/identity/next?name=" + UrlUtils.escapeDataString(name),
        HttpMethods.POST, new RavenJObject(), credentials, convention).addOperationHeaders(operationsHeaders));
    try {
      RavenJToken ravenJToken = jsonRequest.readResponseJson();
      return ravenJToken.value(Long.class, "Value");
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  private PutResult directPut(RavenJObject metadata, String key, UUID etag, RavenJObject document, String operationUrl) throws ServerClientException {
    if (metadata == null) {
      metadata = new RavenJObject();
    }
    HttpMethods method = StringUtils.isNotEmpty(key) ? HttpMethods.PUT : HttpMethods.POST;
    if (etag != null) {
      metadata.set("ETag", new RavenJValue(etag.toString()));
    }
    if (key != null) {
      key = UrlUtils.escapeDataString(key);
    }

    String requestUrl = operationUrl + "/docs/" + ((key != null) ? key : "");

    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, requestUrl, method, metadata, credentials, convention).addOperationHeaders(operationsHeaders)).addReplicationStatusHeaders(url, operationUrl,
        replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      jsonRequest.write(document.toString());
      RavenJObject responseJson = (RavenJObject) jsonRequest.readResponseJson();

      return new PutResult(responseJson.value(String.class, "Key"), responseJson.value(UUID.class, "ETag"));
    } catch (Exception e) {
      if (e instanceof HttpOperationException) {
        HttpOperationException httpException = (HttpOperationException) e;
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

    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + UrlUtils.escapeDataString(key), HttpMethods.PUT, metadata, credentials, convention)).addReplicationStatusHeaders(url,
        operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());
    try {
      jsonRequest.write(data);
      jsonRequest.executeRequest();
    } catch (HttpOperationException e) {
      if (e.getStatusCode() != HttpStatus.SC_INTERNAL_SERVER_ERROR) {
        throw e;
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        IOUtils.copy(e.getHttpResponse().getEntity().getContent(), baos);
      } catch (IOException e1) {
        throw new ServerClientException(e1);
      }
      throw new ServerClientException("Internal server error: " + baos.toString());
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  private List<JsonDocument> directStartsWith(String serverUrl, String keyPrefix, String matches, int start, int pageSize, boolean metadataOnly) throws ServerClientException {
    String actualUrl = serverUrl + String.format("/docs?startsWith=%s&matches=%s&start=%d&pageSize=%d", UrlUtils.escapeDataString(keyPrefix), StringUtils.trimToEmpty(matches), start, pageSize);
    if (metadataOnly) {
      actualUrl += "&metadata-only=true";
    }
    RavenJObject metadata = new RavenJObject();

    addTransactionInformation(metadata);

    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, actualUrl, HttpMethods.GET, metadata, credentials, convention).addOperationHeaders(operationsHeaders)).addReplicationStatusHeaders(url, serverUrl,
        replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      RavenJToken responseJson = jsonRequest.readResponseJson();
      return SerializationHelper.ravenJObjectsToJsonDocuments(responseJson);
    } catch (HttpOperationException e) {
      if (e.getStatusCode() == HttpStatus.SC_CONFLICT) {
        throwConcurrencyException(e);
      }
      throw e;
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  protected void directUpdateAttachmentMetadata(String key, RavenJObject metadata, UUID etag, String operationUrl) {
    if (etag != null) {
      metadata.set("Etag", new RavenJValue(etag.toString()));
    }

    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + UrlUtils.escapeDataString(key), HttpMethods.POST, metadata, credentials, convention)).addReplicationStatusHeaders(url,
        operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      jsonRequest.executeRequest();
    } catch (HttpOperationException e) {
      if (e.getStatusCode() != HttpStatus.SC_INTERNAL_SERVER_ERROR) {
        throw e;
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        IOUtils.copy(e.getHttpResponse().getEntity().getContent(), baos);
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
    return executeWithReplication(HttpMethods.GET, new Function1<String, RavenJToken>() {
      @Override
      public RavenJToken apply(String serverUrl) {
        RavenJObject metadata = new RavenJObject();
        addTransactionInformation(metadata);
        HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(ServerClient.this, serverUrl + requestUrl, HttpMethods.GET, metadata, credentials,
            convention).addOperationHeaders(operationsHeaders));
        try {
          return jsonRequest.readResponseJson();
        } catch (Exception e) {
          throw new ServerClientException(e.getMessage(), e);
        }
      }
    });
  }

  private <T> T executeWithReplication(HttpMethods method, Function1<String, T> operation) throws ServerClientException {
    int currentRequest = convention.incrementRequestCount();
    return replicationInformer.executeWithReplication(method, url, currentRequest, readStripingBase, operation);
  }

  public boolean isInFailoverMode() {
    return replicationInformer.getFailureCount(url) > 0;
  }

  public AutoCloseable forceReadFromMaster() {
    final int old = readStripingBase;
    readStripingBase = -1;
    return new AutoCloseable() {
      @Override
      public void close() throws Exception {
        readStripingBase = old;
      }
    };
  }

  //TODO: public bool InFailoverMode()
  //TODO : private bool AssertNonConflictedDocumentAndCheckIfNeedToReload(RavenJObject docResult)
  //TODO: public BatchResult[] Batch(IEnumerable<ICommandData> commandDatas)
  //TODO: private BatchResult[] DirectBatch(IEnumerable<ICommandData> commandDatas, string operationUrl)
  //TODO: private T RetryOperationBecauseOfConflict<T>(IEnumerable<RavenJObject> docResults, T currentResult, Func<T> nextTry)
  //TODO: public void Commit(Guid txId)
  //TODO: private void DirectCommit(Guid txId, string operationUrl)
  //TODO: public void Rollback(Guid txId)
  //TODO: public byte[] PromoteTransaction(Guid fromTxId)
  //TODO: private byte[] DirectPromoteTransaction(Guid fromTxId, string operationUrl)
  //TODO:private void DirectRollback(Guid txId, string operationUrl)
  //TODO: public IDatabaseCommands With(ICredentials credentialsForSession)
  //TODO: public ILowLevelBulkInsertOperation GetBulkInsertOperation(BulkInsertOptions options)
  //TODO: public IndexDefinition[] GetIndexes(int start, int pageSize)
  //TODO: public void ResetIndex(string name)
  //TODO: private object DirectResetIndex(string name, string operationUrl)
  //TODO: private string[] DirectGetIndexNames(int start, int pageSize, string operationUrl)
  //TODO: public IndexDefinition GetIndex(string name)
  //TODO: private IndexDefinition DirectGetIndex(string indexName, string operationUrl)
  //TODO: public HttpJsonRequest CreateRequest(string method, string requestUrl, bool disableRequestCompression = false)
  //TODO: private void HandleReplicationStatusChanges(NameValueCollection headers, string primaryUrl, string currentUrl)
  //TODO: private ConflictException TryResolveConflictOrCreateConcurrencyException(string key, RavenJObject conflictsDoc, Guid etag)
  //TODO public void DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale)
  //TODO: public string[] GetIndexNames(int start, int pageSize)
  //TODO: public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests)
  //TODO: public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch)
  //TODO: public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale)
  //TODO: public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, bool allowStale)
  //TODO: private void UpdateByIndexImpl(string indexName, IndexQuery queryToUpdate, bool allowStale, String requestData, String method)
  //TODO: public void DeleteByIndex(string indexName, IndexQuery queryToDelete)
  //TODO: public SuggestionQueryResult Suggest(string index, SuggestionQuery suggestionQuery)
  //TODO: public MultiLoadResult MoreLikeThis(MoreLikeThisQuery query)
  //TODO: public DatabaseStatistics GetStatistics()
  //TODO: public GetResponse[] MultiGet(GetRequest[] requests)
  //TODO: public IEnumerable<string> GetTerms(string index, string field, string fromValue, int pageSize)
  //TODO: public FacetResults GetFacets(string index, IndexQuery query, string facetSetupDoc, int start, int? pageSize)
  //TODO: public void Patch(string key, PatchRequest[] patches)
  //TODO: public void Patch(string key, ScriptedPatchRequest patch)
  //TODO: public void Patch(string key, PatchRequest[] patches, Guid? etag)
  //TODO: public void Patch(string key, ScriptedPatchRequest patch, Guid? etag)
  //TODO: public IDisposable DisableAllCaching()
  //TODO: public ProfilingInformation ProfilingInformation
  //TODO: public RavenJToken GetOperationStatus(long id)
  //TODO: public IDisposable Expect100Continue()
  //TODO :public string PutIndex(string name, IndexDefinition definition)
  //TODO: public string DirectPutIndex(string name, string operationUrl, bool overwrite, IndexDefinition definition)
  //TODO: public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef)
  //TODO: public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, bool overwrite)
  //TODO: public QueryResult Query(string index, IndexQuery query, string[] includes, bool metadataOnly = false, bool indexEntriesOnly = false)
  //TODO: private QueryResult DirectQuery(string index, IndexQuery query, string operationUrl, string[] includes, bool metadataOnly, bool includeEntries)
  //TODO: public void DeleteIndex(string name)
  //TODO: private void DirectDeleteIndex(string name, string operationUrl)

  @Override
  public JsonDocumentMetadata head(final String key) {
    ensureIsNotNullOrEmpty(key, "key");
    return executeWithReplication(HttpMethods.HEAD, new Function1<String, JsonDocumentMetadata>() {
      @Override
      public JsonDocumentMetadata apply(String u) {
        return directHead(u, key);
      }
    });
  }

  protected JsonDocumentMetadata directHead(String serverUrl, String key) {
    RavenJObject metadata = new RavenJObject();
    addTransactionInformation(metadata);

    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(ServerClient.this, serverUrl + "/docs/" + key, HttpMethods.HEAD, new RavenJObject(), credentials, convention).addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, serverUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {

      RavenJToken responseJson = jsonRequest.readResponseJson();
      return SerializationHelper.deserializeJsonDocumentMetadata(responseJson);
    } catch (HttpOperationException e) {
      if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        return null;
      } else if (e.getStatusCode() == HttpStatus.SC_CONFLICT) {
        //TODO: throw conflict exception
      }
      throw new ServerClientException(e);
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  public IDatabaseCommands forDatabase(String database) {
    String databaseUrl = MultiDatabase.getRootDatabaseUrl(url);
    databaseUrl = url + "/databases/" + database;
    if (databaseUrl.equals(url)) {
      return this;
    }
    return new ServerClient(databaseUrl, convention, credentials, replicationInformer, jsonRequestFactory, currentSessionId, conflictListeners);
  }

  public IDatabaseCommands forSystemDatabase() {
    String databaseUrl = MultiDatabase.getRootDatabaseUrl(url);
    if (databaseUrl.equals(url)) {
      return this;
    }
    return new ServerClient(databaseUrl, convention, credentials, replicationInformer, jsonRequestFactory, currentSessionId, conflictListeners);
  }

  @Override
  public JsonDocument get(final String key) throws ServerClientException {
    ensureIsNotNullOrEmpty(key, "key");
    return executeWithReplication(HttpMethods.GET, new Function1<String, JsonDocument>() {
      @Override
      public JsonDocument apply(String u) {
        return directGet(u, key);
      }
    });
  }

  @Override
  public MultiLoadResult get(String[] ids, String[] includes) {
    return get(ids, includes, false);
  }

  @Override
  public MultiLoadResult get(final String[] ids, final String[] includes, final boolean metadataOnly) {
    return executeWithReplication(HttpMethods.GET, new Function1<String, MultiLoadResult>() {

      @Override
      public MultiLoadResult apply(String u) {
        return directGet(ids, u, includes, metadataOnly);
      }
    });
  }

  @Override
  public Attachment getAttachment(final String key) {
    return executeWithReplication(HttpMethods.GET, new Function1<String, Attachment>() {
      @Override
      public Attachment apply(String operationUrl) {
        return directGetAttachment(HttpMethods.GET, key, operationUrl);
      }
    });
  }

  public List<Attachment> getAttachmentHeadersStartingWith(final String idPrefix, final int start, final int pageSize) {
    return executeWithReplication(HttpMethods.GET, new Function1<String, List<Attachment>>() {

      @Override
      public List<Attachment> apply(String operationUrl) {
        return directGetAttachmentHeadersStartingWith(HttpMethods.GET, idPrefix, start, pageSize, operationUrl);
      }
    });
  }

  @Override
  public List<String> getDatabaseNames(int pageSize) {
    return getDatabaseNames(pageSize, 0);
  }

  @Override
  public List<String> getDatabaseNames(int pageSize, int start) {
    String url = RavenUrlExtensions.databases("", pageSize, start);
    url = RavenUrlExtensions.noCache(url);
    RavenJArray json = (RavenJArray) executeGetRequest(url);
    List<String> result = new ArrayList<>();
    for (RavenJToken token : json) {
      RavenJValue value = (RavenJValue) token;
      result.add((String) value.getValue());
    }
    return result;
  }

  @Override
  public List<JsonDocument> getDocuments(int start, int pageSize) {
    return getDocuments(start, pageSize, false);
  }

  @Override
  public List<JsonDocument> getDocuments(final int start, final int pageSize, final boolean metadataOnly) {
    return executeWithReplication(HttpMethods.GET, new Function1<String, List<JsonDocument>>() {
      @Override
      public List<JsonDocument> apply(String url) {
        String requestUri = url + "/docs?start=" + start + "&pageSize=" + pageSize;
        if (metadataOnly) {
          requestUri += "&metadata-only=true";
        }

        HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(ServerClient.this, RavenUrlExtensions.noCache(requestUri), HttpMethods.GET,
            new RavenJObject(), credentials, convention).addOperationHeaders(operationsHeaders));

        try {
          RavenJToken responseJson = jsonRequest.readResponseJson();
          return SerializationHelper.ravenJObjectsToJsonDocuments(responseJson);
        } catch (Exception e) {
          throw new ServerClientException(e);
        }
      }
    });
  }

  /**
   * @return the url
   */
  public String getUrl() {
    return url;
  }

  @Override
  public Attachment headAttachment(final String key) {
    return executeWithReplication(HttpMethods.HEAD, new Function1<String, Attachment>() {
      @Override
      public Attachment apply(String operationUrl) {
        return directGetAttachment(HttpMethods.HEAD, key, operationUrl);
      }
    });
  }

  public Long nextIdentityFor(final String name) {
    return executeWithReplication(HttpMethods.POST, new Function1<String, Long>() {
      @Override
      public Long apply(String url) {
        return directNextIdentityFor(name, url);
      }
    });
  }

  @Override
  public PutResult put(final String key, final UUID etag, final RavenJObject document, final RavenJObject metadata) throws ServerClientException {
    return executeWithReplication(HttpMethods.PUT, new Function1<String, PutResult>() {
      @Override
      public PutResult apply(String u) {
        return directPut(metadata, key, etag, document, u);
      }
    });
  }

  @Override
  public void putAttachment(final String key, final UUID etag, final InputStream data, final RavenJObject metadata) {
    executeWithReplication(HttpMethods.PUT, new Function1<String, Void>() {
      @Override
      public Void apply(String operationUrl) {
        directPutAttachment(key, metadata, etag, data, operationUrl);
        return null;
      }
    });
  }

  @Override
  public List<JsonDocument> startsWith(final String keyPrefix, final String matches, final int start, final int pageSize) throws ServerClientException {
    return startsWith(keyPrefix, matches, start, pageSize, false);
  }

  @Override
  public List<JsonDocument> startsWith(final String keyPrefix, final String matches, final int start, final int pageSize, final boolean metadataOnly) throws ServerClientException {
    ensureIsNotNullOrEmpty(keyPrefix, "keyPrefix");
    return executeWithReplication(HttpMethods.GET, new Function1<String, List<JsonDocument>>() {
      @Override
      public List<JsonDocument> apply(String u) {
        return directStartsWith(u, keyPrefix, matches, start, pageSize, metadataOnly);
      }
    });
  }

  private ConcurrencyException throwConcurrencyException(Exception e) {
    UUID expectedEtag = null;
    UUID actualEtag = null;
    //TODO: implement me!
    return new ConcurrencyException(expectedEtag, actualEtag, e);
  }

  @Override
  public void updateAttachmentMetadata(final String key, final UUID etag, final RavenJObject metadata) {
    executeWithReplication(HttpMethods.POST, new Function1<String, Void>() {
      @Override
      public Void apply(String operationUrl) {
        directUpdateAttachmentMetadata(key, metadata, etag, operationUrl);
        return null;
      }
    });
  }

  @Override
  public String urlFor(String documentKey) {
    return url + "/docs/" + documentKey;
  }

}
