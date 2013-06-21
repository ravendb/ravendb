package raven.client.connection;

import java.io.ByteArrayInputStream;
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
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;

import raven.abstractions.basic.EventHandler;
import raven.abstractions.basic.Holder;
import raven.abstractions.closure.Action3;
import raven.abstractions.closure.Function1;
import raven.abstractions.data.Attachment;
import raven.abstractions.data.Constants;
import raven.abstractions.data.Etag;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.JsonDocumentMetadata;
import raven.abstractions.data.MultiLoadResult;
import raven.abstractions.data.PutResult;
import raven.abstractions.exceptions.ConcurrencyException;
import raven.abstractions.exceptions.HttpOperationException;
import raven.abstractions.exceptions.ServerClientException;
import raven.abstractions.extensions.MetadataExtensions;
import raven.abstractions.json.linq.RavenJArray;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.abstractions.json.linq.RavenJValue;
import raven.client.connection.ReplicationInformer.FailoverStatusChangedEventArgs;
import raven.client.connection.profiling.ProfilingInformation;
import raven.client.document.DocumentConvention;
import raven.client.exceptions.ConflictException;
import raven.client.extensions.MultiDatabase;
import raven.client.listeners.IDocumentConflictListener;
import raven.client.utils.UrlUtils;

//TODO: finish me
public class ServerClient implements IDatabaseCommands {

  private String url;
  private final DocumentConvention convention;
  private final Credentials credentials;
  private final Function1<String, ReplicationInformer> replicationInformerGetter;
  private String databaseName;
  private final ReplicationInformer replicationInformer;
  private final HttpJsonRequestFactory jsonRequestFactory;
  private final UUID currentSessionId;
  private final IDocumentConflictListener[] conflictListeners;
  private final ProfilingInformation profilingInformation;
  private int readStripingBase;

  private boolean resolvingConflict;
  private boolean resolvingConflictRetries;

  private Map<String, String> operationsHeaders;

  public void addFailoverStatusChanged(EventHandler<FailoverStatusChangedEventArgs> event) {
    replicationInformer.addFailoverStatusChanged(event);
  }

  public void removeFailoverStatusChanged(EventHandler<FailoverStatusChangedEventArgs> event) {
    replicationInformer.removeFailoverStatusChanged(event);
  }




  public ServerClient(String url, DocumentConvention convention, Credentials credentials, Function1<String, ReplicationInformer> replicationInformerGetter,  String databaseName,
      HttpJsonRequestFactory httpJsonRequestFactory, UUID currentSessionId, IDocumentConflictListener[] conflictListeners) {
    this.profilingInformation = ProfilingInformation.createProfilingInformation(currentSessionId);
    this.credentials = credentials;
    this.replicationInformerGetter = replicationInformerGetter;
    this.replicationInformer = replicationInformerGetter.apply(databaseName);
    this.jsonRequestFactory = httpJsonRequestFactory;
    this.currentSessionId = currentSessionId;
    this.conflictListeners = conflictListeners;
    this.url = url;

    if (url.endsWith("/")) {
      this.url = url.substring(0, url.length() - 1);
    }
    this.convention = convention;
    operationsHeaders = new HashMap<>();
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

  /**
   * @param operationsHeaders the operationsHeaders to set
   */
  public void setOperationsHeaders(Map<String, String> operationsHeaders) {
    this.operationsHeaders = operationsHeaders;
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

  public RavenJToken executeGetRequest(final String requestUrl) {
    ensureIsNotNullOrEmpty(requestUrl, "url");
    return executeWithReplication(HttpMethods.GET, new Function1<String, RavenJToken>() {
      @Override
      public RavenJToken apply(String serverUrl) {
        RavenJObject metadata = new RavenJObject();
        addTransactionInformation(metadata);
        HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
            new CreateHttpJsonRequestParams(ServerClient.this, serverUrl + requestUrl, HttpMethods.GET, metadata, credentials, convention).
            addOperationHeaders(operationsHeaders));
        try {
          return jsonRequest.readResponseJson();
        } catch (Exception e) {
          throw new ServerClientException(e.getMessage(), e);
        }
      }
    });
  }

  public HttpJsonRequest createRequest(HttpMethods method, String requestUrl) {
    return createRequest(method, requestUrl, false);
  }

  public HttpJsonRequest createRequest(HttpMethods method, String requestUrl, boolean disableRequestCompression) {
    RavenJObject metadata = new RavenJObject();
    addTransactionInformation(metadata);
    CreateHttpJsonRequestParams createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, url + requestUrl, method, metadata, credentials, convention).addOperationHeaders(operationsHeaders);
    createHttpJsonRequestParams.setDisableRequestCompression(disableRequestCompression);
    return jsonRequestFactory.createHttpJsonRequest(createHttpJsonRequestParams);
  }

  private <T> T executeWithReplication(HttpMethods method, Function1<String, T> operation) throws ServerClientException {
    int currentRequest = convention.incrementRequestCount();
    return replicationInformer.executeWithReplication(method, url, currentRequest, readStripingBase, operation);
  }

  public boolean isInFailoverMode() {
    return replicationInformer.getFailureCount(url) > 0;
  }

  public JsonDocument directGet(String serverUrl, String key) {
    return directGet(serverUrl, key, null);
  }

  /**
   * Perform a direct get for a document with the specified key on the specified server URL.
   * @param serverUrl
   * @param key
   * @return
   */
  public JsonDocument directGet(String serverUrl, String key, String transform) throws ServerClientException {
    if (key.length() > 127) {
      MultiLoadResult multiLoadResult = directGet(new String[] { key}, serverUrl, new String[0], null, new HashMap<String, RavenJToken>(), false);
      List<RavenJObject> results = multiLoadResult.getResults();
      if (results.size() == 0) {
        return null;
      } else {
        return SerializationHelper.ravenJObjectToJsonDocument(results.get(0));
      }
    }

    RavenJObject metadata = new RavenJObject();
    String actualUrl = serverUrl + "/docs/" + UrlUtils.escapeDataString(key);
    if (StringUtils.isNotEmpty(transform)) {
      actualUrl += "?=" + UrlUtils.escapeDataString(transform);
    }
    addTransactionInformation(metadata);

    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, actualUrl, HttpMethods.GET, metadata, credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, serverUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());
    try {
      RavenJToken responseJson = request.readResponseJson();

      String docKey = request.getResponseHeaders().get(Constants.DOCUMENT_ID_FIELD_NAME);
      if (docKey == null) {
        docKey = key;
      }
      docKey = UrlUtils.unescapeDataString(docKey);
      request.getResponseHeaders().remove(Constants.DOCUMENT_ID_FIELD_NAME);
      return SerializationHelper.deserializeJsonDocument(docKey, responseJson, request.getResponseHeaders(), request.getResponseStatusCode());

    } catch (HttpOperationException e) {
      HttpOperationException httpException = e;
      HttpResponse httpWebResponse = e.getHttpResponse();
      if (httpWebResponse.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        return null;
      } else if (httpWebResponse.getStatusLine().getStatusCode() == HttpStatus.SC_CONFLICT) {
        /*FIXME: rewrite
        var conflicts = new StreamReader(httpWebResponse.GetResponseStreamWithHttpDecompression());
        var conflictsDoc = RavenJObject.Load(new RavenJsonTextReader(conflicts));
        var etag = httpWebResponse.GetEtagHeader();

        var concurrencyException = TryResolveConflictOrCreateConcurrencyException(key, conflictsDoc, etag);
        if (concurrencyException == null)
        {
          if (resolvingConflictRetries)
            throw new InvalidOperationException("Encountered another conflict after already resolving a conflict. Conflict resultion cannot recurse.");

          resolvingConflictRetries = true;
          try
          {
            return DirectGet(serverUrl, key);
          }
          finally
          {
            resolvingConflictRetries = false;
          }
        }
        throw concurrencyException;*/
      }
      throw new ServerClientException(e);
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  private void handleReplicationStatusChanges(Map<String, String> headers, String primaryUrl, String currentUrl) {
    /* FIXME: rewrite me
     * if (!primaryUrl.Equals(currentUrl, StringComparison.OrdinalIgnoreCase))
      {
        var forceCheck = headers[Constants.RavenForcePrimaryServerCheck];
        bool shouldForceCheck;
        if (!string.IsNullOrEmpty(forceCheck) && bool.TryParse(forceCheck, out shouldForceCheck))
        {
          this.replicationInformer.ForceCheck(primaryUrl, shouldForceCheck);
        }
      }
     */
  }

  private ConflictException tryResolveConflictOrCreateConcurrencyException(String key, RavenJObject conflictsDoc, Etag etag) {
    RavenJArray ravenJArray = conflictsDoc.value(RavenJArray.class, "Conflicts");
    if (ravenJArray == null) {
      throw new IllegalArgumentException("Could not get conflict ids from conflicted document, are you trying to resolve a conflict when using metadata-only?");
    }

    List<String> conflictIds = new ArrayList<>();
    for (RavenJToken token: ravenJArray) {
      conflictIds.add(token.value(String.class));
    }

    if (conflictListeners.length > 0 && resolvingConflict == false) {
      resolvingConflict = true;
      try {
        MultiLoadResult multiLoadResult = get(conflictIds.toArray(new String[0]), null);

        List<JsonDocument> results = new ArrayList<>();
        for (RavenJObject r: multiLoadResult.getResults()) {
          results.add(SerializationHelper.toJsonDocument(r));
        }

        for(IDocumentConflictListener conflictListener: conflictListeners) {
          Holder<JsonDocument> resolvedDocument = new Holder<>();
          if (conflictListener.tryResolveConflict(key, results, resolvedDocument)) {
            put(key, etag, resolvedDocument.value.getDataAsJson(), resolvedDocument.value.getMetadata());
            return null;
          }
        }
      }
      finally {
        resolvingConflict = false;
      }
    }

    ConflictException conflictException = new ConflictException("Conflict detected on " + key +
        ", conflict must be resolved before the document will be accessible", true);
    conflictException.setConflictedVersionIds(conflictIds.toArray(new String[0]));
    conflictException.setEtag(etag);
    return conflictException;
  }

  private void ensureIsNotNullOrEmpty(String key, String argName) {
    if (key == null || "".equals(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty " + argName);
    }
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
        try {
          RavenJToken responseJson = jsonRequestFactory
              .createHttpJsonRequest(new CreateHttpJsonRequestParams(ServerClient.this, RavenUrlExtensions.noCache(requestUri), HttpMethods.GET,
                  new RavenJObject(), credentials, convention)
              .addOperationHeaders(operationsHeaders)).readResponseJson();

          return SerializationHelper.ravenJObjectsToJsonDocuments(responseJson);
        } catch (Exception e) {
          throw new ServerClientException(e);
        }
      }
    });
  }

  @Override
  public PutResult put(final String key, final Etag etag, final RavenJObject document, final RavenJObject metadata) throws ServerClientException {
    return executeWithReplication(HttpMethods.PUT, new Function1<String, PutResult>() {
      @Override
      public PutResult apply(String u) {
        return directPut(metadata, key, etag, document, u);
      }
    });
  }

  private List<JsonDocument> directStartsWith(String operationUrl, String keyPrefix, String matches, int start, int pageSize, boolean metadataOnly) throws ServerClientException {
    RavenJObject metadata = new RavenJObject();
    addTransactionInformation(metadata);
    String actualUrl = operationUrl + String.format("/docs?startsWith=%s&matches=%s&start=%d&pageSize=%d", UrlUtils.escapeDataString(keyPrefix),
        UrlUtils.escapeDataString(StringUtils.trimToEmpty(matches)), start, pageSize);
    if (metadataOnly) {
      actualUrl += "&metadata-only=true";
    }

    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, actualUrl, HttpMethods.GET, metadata, credentials, convention).
        addOperationHeaders(operationsHeaders)).
        addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      RavenJToken responseJson = jsonRequest.readResponseJson();
      return SerializationHelper.ravenJObjectsToJsonDocuments(responseJson);
    } catch (HttpOperationException e) {
      if (e.getStatusCode() == HttpStatus.SC_CONFLICT) {
        throw throwConcurrencyException(e);
      }
      throw e;
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  private PutResult directPut(RavenJObject metadata, String key, Etag etag, RavenJObject document, String operationUrl) throws ServerClientException {
    if (metadata == null) {
      metadata = new RavenJObject();
    }
    HttpMethods method = StringUtils.isNotEmpty(key) ? HttpMethods.PUT : HttpMethods.POST;
    addTransactionInformation(metadata);
    if (etag != null) {
      metadata.set("ETag", new RavenJValue(etag.toString()));
    }
    if (key != null) {
      key = UrlUtils.escapeDataString(key);
    }

    String requestUrl = operationUrl + "/docs/" + ((key != null) ? key : "");

    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, requestUrl, method, metadata, credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      jsonRequest.write(document.toString());
      RavenJObject responseJson = (RavenJObject) jsonRequest.readResponseJson();

      return new PutResult(responseJson.value(String.class, "Key"), responseJson.value(Etag.class, "ETag"));
    } catch (Exception e) {
      if (e instanceof HttpOperationException) {
        HttpOperationException httpException = (HttpOperationException) e;
        if (httpException.getStatusCode() == HttpStatus.SC_CONFLICT) {
          throw throwConcurrencyException(httpException);
        }
      }
      throw new ServerClientException(e);
    }
  }

  protected void addTransactionInformation(RavenJObject metadata) {
    /*FIXME: rewrite me!
    if (convention.EnlistInDistributedTransactions == false)
      return;

    var transactionInformation = RavenTransactionAccessor.GetTransactionInformation();
    if (transactionInformation == null)
      return;

    string txInfo = string.Format("{0}, {1}", transactionInformation.Id, transactionInformation.Timeout);
    metadata["Raven-Transaction-Information"] = new RavenJValue(txInfo);
     */
  }

  @Override
  public void delete(final String key, final Etag etag) throws ServerClientException {
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
  public void putAttachment(final String key, final Etag etag, final InputStream data, final RavenJObject metadata) {
    executeWithReplication(HttpMethods.PUT, new Function1<String, Void>() {
      @Override
      public Void apply(String operationUrl) {
        directPutAttachment(key, metadata, etag, data, operationUrl);
        return null;
      }
    });
  }

  @Override
  public void updateAttachmentMetadata(final String key, final Etag etag, final RavenJObject metadata) {
    executeWithReplication(HttpMethods.POST, new Function1<String, Void>() {
      @Override
      public Void apply(String operationUrl) {
        directUpdateAttachmentMetadata(key, metadata, etag, operationUrl);
        return null;
      }
    });
  }

  protected void directUpdateAttachmentMetadata(String key, RavenJObject metadata, Etag etag, String operationUrl) {
    if (etag != null) {
      metadata.set("Etag", new RavenJValue(etag.toString()));
    }

    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + key, HttpMethods.POST, metadata, credentials, convention))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

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
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
      }
      throw new ServerClientException("Internal server error: " + baos.toString());
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  protected void directPutAttachment(String key, RavenJObject metadata, Etag etag, InputStream data, String operationUrl) {
    if (etag != null) {
      metadata.set("Etag", new RavenJValue(etag.toString()));
    }

    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + key, HttpMethods.PUT, metadata, credentials, convention))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());
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
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
      }
      throw new ServerClientException("Internal server error: " + baos.toString());
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  public List<Attachment> getAttachmentHeadersStartingWith(final String idPrefix, final int start, final int pageSize) {
    return executeWithReplication(HttpMethods.GET, new Function1<String, List<Attachment>>() {

      @Override
      public List<Attachment> apply(String operationUrl) {
        return directGetAttachmentHeadersStartingWith(HttpMethods.GET, idPrefix, start, pageSize, operationUrl);
      }
    });
  }

  protected List<Attachment> directGetAttachmentHeadersStartingWith(HttpMethods method, String idPrefix, int start, int pageSize, String operationUrl) {
    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/?startsWith" + idPrefix + "&start=" + start + "&pageSize=" + pageSize, method, new RavenJObject(), credentials,
            convention))
            .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());
    try {
      RavenJToken responseJson = jsonRequest.readResponseJson();
      // this method not-exists in .net version
      return SerializationHelper.deserializeAttachements(responseJson);

    } catch (Exception e) {
      throw new ServerClientException(e);
    }
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

  @Override
  public Attachment headAttachment(final String key) {
    return executeWithReplication(HttpMethods.HEAD, new Function1<String, Attachment>() {
      @Override
      public Attachment apply(String operationUrl) {
        return directGetAttachment(HttpMethods.HEAD, key, operationUrl);
      }
    });
  }

  protected Attachment directGetAttachment(HttpMethods method, String key, String operationUrl) {
    HttpJsonRequest webRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + key, method, new RavenJObject(), credentials, convention))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    byte[] data = null;
    boolean canGetData;
    try {
      int len;
      if (HttpMethods.GET == method) {
        data = webRequest.readResponseBytes();
        len = data.length;
        canGetData = true;
      } else {
        webRequest.executeRequest();
        len = Integer.parseInt(webRequest.getResponseHeaders().get("Content-Length"));
        canGetData = false;
      }
      handleReplicationStatusChanges(webRequest.getResponseHeaders(), url, operationUrl);

      return new Attachment(canGetData, data, len, MetadataExtensions.filterHeadersAttachment(webRequest.getResponseHeaders()),
          HttpExtensions.getEtagHeader(webRequest), null);

    } catch (HttpOperationException e) {
      if (e.getStatusCode() == HttpStatus.SC_CONFLICT) {
        /*FIXME: rewrite
         * var conflictsDoc = RavenJObject.Load(new BsonReader(httpWebResponse.GetResponseStreamWithHttpDecompression()));
          var conflictIds = conflictsDoc.Value<RavenJArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

          throw new ConflictException("Conflict detected on " + key +
                        ", conflict must be resolved before the attachment will be accessible", true)
          {
            ConflictedVersionIds = conflictIds,
            Etag = httpWebResponse.GetEtagHeader()
          };
         */
      } else if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
        return null;
      }
      throw new ServerClientException(e);

    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  @Override
  public void deleteAttachment(final String key, final Etag etag) {
    executeWithReplication(HttpMethods.DELETE, new Function1<String, Void>() {
      @Override
      public Void apply(String operationUrl) {
        directDeleteAttachment(key, etag, operationUrl);
        return null;
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
      result.add(token.value(String.class));
    }
    return result;
  }

  public Map<String, RavenJToken> getDatabases(int pageSize) {
    return getDatabases(pageSize, 0);
  }

  public Map<String, RavenJToken> getDatabases(int pageSize, int start) {
    String url = RavenUrlExtensions.databases("", pageSize, start);
    url = RavenUrlExtensions.noCache(url);
    RavenJArray json = (RavenJArray) executeGetRequest(url);

    Map<String, RavenJToken> result = new HashMap<>();
    for (RavenJToken token: json) {
      result.put(token.value(RavenJObject.class, "@metadata").value(String.class, "@id").replace("Raven/Databases/", ""), token);
    }

    return result;
  }

  protected void directDeleteAttachment(String key, Etag etag, String operationUrl) {
    RavenJObject metadata = new RavenJObject();
    if (etag != null) {
      metadata.add("ETag", RavenJToken.fromObject(etag.toString()));
    }
    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + UrlUtils.escapeDataString(key), HttpMethods.DELETE, metadata, credentials, convention))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());
    try {
      jsonRequest.executeRequest();
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  //TODO: public string[] GetIndexNames(int start, int pageSize)

  //TODO: public IndexDefinition[] GetIndexes(int start, int pageSize)

  //TODO: public TransformerDefinition[] GetTransformers(int start, int pageSize)

  //TODO: public TransformerDefinition GetTransformer(string name)

  //TODO: public void DeleteTransformer(string name)

  //TODO: private void DirectDeleteTransformer(string name, string operationUrl)

  //TODO: private TransformerDefinition DirectGetTransformer(string transformerName, string operationUrl)

  //TODO: public void ResetIndex(string name)

  //TODO: private object DirectResetIndex(string name, string operationUrl)

  //TODO: private string[] DirectGetIndexNames(int start, int pageSize, string operationUrl)

  //TODO: public IndexDefinition GetIndex(string name)

  //TODO: private IndexDefinition DirectGetIndex(string indexName, string operationUrl)


  private void directDelete(String serverUrl, String key, Etag etag) throws ServerClientException {
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

  protected class HandleReplicationStatusChangesCallback implements Action3<Map<String, String>, String, String> {
    @Override
    public void apply(Map<String, String> headers, String primaryUrl, String currentUrl) {
      handleReplicationStatusChanges(headers, primaryUrl, currentUrl);
    }
  }

  private ConcurrencyException throwConcurrencyException(HttpOperationException e) {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      IOUtils.copy(e.getHttpResponse().getEntity().getContent(), baos);
    } catch (IOException e1) {
      throw new ServerClientException(e1);
    } finally {
      EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
    }

    RavenJToken ravenJToken = RavenJObject.tryLoad(new ByteArrayInputStream(baos.toByteArray()));

    return new ConcurrencyException(ravenJToken.value(Etag.class, "expectedETag"), ravenJToken.value(Etag.class, "actualETag"), ravenJToken.value(String.class, "error"), e);

  }

  //TODO: public string PutIndex(string name, IndexDefinition definition)

  //TODO: public string PutTransformer(string name, TransformerDefinition indexDef)

  //TODO: public string PutIndex(string name, IndexDefinition definition, bool overwrite)

  //TODO: public string DirectPutTransformer(string name, string operationUrl, TransformerDefinition definition)

  //TODO: public string DirectPutIndex(string name, string operationUrl, bool overwrite, IndexDefinition definition)

  //TODO: public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef)

  //TODO: public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, bool overwrite)

  //TODO: public QueryResult Query(string index, IndexQuery query, string[] includes, bool metadataOnly = false, bool indexEntriesOnly = false)

  //TODO: public IEnumerator<RavenJObject> StreamQuery(string index, IndexQuery query, out QueryHeaderInformation queryHeaderInfo)

  //TODO: public IEnumerator<RavenJObject> StreamDocs(Etag fromEtag, string startsWith, string matches, int start, int pageSize)

  //TODO: private static IEnumerator<RavenJObject> YieldStreamResults(WebResponse webResponse)

  //TODO: private QueryResult DirectQuery(string index, IndexQuery query, string operationUrl, string[] includes, bool metadataOnly, bool includeEntries)

  //TODO: public void DeleteIndex(string name)

  //TODO: private void DirectDeleteIndex(string name, string operationUrl)



  public MultiLoadResult get(final String[] ids, final String[] includes) {
    return get(ids, includes, null, null, false);
  }

  public MultiLoadResult get(final String[] ids, final String[] includes, final String transformer) {
    return get(ids, includes, transformer, null, false);
  }

  public MultiLoadResult get(final String[] ids, final String[] includes, final String transformer, final Map<String, RavenJToken> queryInputs) {
    return get(ids, includes, transformer, queryInputs, false);
  }

  public MultiLoadResult get(final String[] ids, final String[] includes, final String transformer, final Map<String, RavenJToken> queryInputs, final boolean metadataOnly) {
    return executeWithReplication(HttpMethods.GET, new Function1<String, MultiLoadResult>() {

      @Override
      public MultiLoadResult apply(String u) {
        return directGet(ids, u, includes, transformer, queryInputs != null ? queryInputs : new HashMap<String, RavenJToken>(), metadataOnly);
      }
    });
  }


  protected MultiLoadResult directGet(String[] ids, String operationUrl, String[] includes, String transformer, Map<String, RavenJToken> queryInputs, boolean metadataOnly) {
   /*FIXME
    * var path = operationUrl + "/queries/?";
      if (metadataOnly)
        path += "&metadata-only=true";
      if (includes != null && includes.Length > 0)
      {
        path += string.Join("&", includes.Select(x => "include=" + x).ToArray());
      }
          if (!string.IsNullOrEmpty(transformer))
              path += "&transformer=" + transformer;


      if (queryInputs != null)
      {
        path = queryInputs.Aggregate(path, (current, queryInput) => current + ("&" + string.Format("qp-{0}={1}", queryInput.Key, queryInput.Value)));
      }
        var metadata = new RavenJObject();
      AddTransactionInformation(metadata);
        var uniqueIds = new HashSet<string>(ids);
      // if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
      // we are fine with that, requests to load that many items are probably going to be rare
      HttpJsonRequest request;
      if (uniqueIds.Sum(x => x.Length) < 1024)
      {
        path += "&" + string.Join("&", uniqueIds.Select(x => "id=" + Uri.EscapeDataString(x)).ToArray());
        request = jsonRequestFactory.CreateHttpJsonRequest(
            new CreateHttpJsonRequestParams(this, path, "GET", metadata, credentials, convention)
              .AddOperationHeaders(OperationsHeaders))
              .AddReplicationStatusHeaders(Url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

      }
      else
      {
        request = jsonRequestFactory.CreateHttpJsonRequest(
            new CreateHttpJsonRequestParams(this, path, "POST", metadata, credentials, convention)
              .AddOperationHeaders(OperationsHeaders))
              .AddReplicationStatusHeaders(Url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

        request.Write(new RavenJArray(uniqueIds).ToString(Formatting.None));
      }


      var result = (RavenJObject)request.ReadResponseJson();

      var results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList();
          var multiLoadResult = new MultiLoadResult
          {
              Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList()
          };

            if(String.IsNullOrEmpty(transformer)) {
                multiLoadResult.Results = ids.Select(id => results.FirstOrDefault(r => string.Equals(r["@metadata"].Value<string>("@id"), id, StringComparison.OrdinalIgnoreCase))).ToList();
      }
            else
            {
                multiLoadResult.Results = results;
            }


      var docResults = multiLoadResult.Results.Concat(multiLoadResult.Includes);

      return RetryOperationBecauseOfConflict(docResults, multiLoadResult, () => DirectGet(ids, operationUrl, includes, transformer, queryInputs, metadataOnly));

    */
    return null;
  }


  //TODO: private T RetryOperationBecauseOfConflict<T>(IEnumerable<RavenJObject> docResults, T currentResult, Func<T> nextTry)

  //TODO :private bool AssertNonConflictedDocumentAndCheckIfNeedToReload(RavenJObject docResult)

  //TODO: public BatchResult[] Batch(IEnumerable<ICommandData> commandDatas)

  //TODO: private BatchResult[] DirectBatch(IEnumerable<ICommandData> commandDatas, string operationUrl)

  //TODO: public void Commit(string txId)

  //TODO: private void DirectCommit(string txId, string operationUrl)

  //TODO public void Rollback(string txId)

  //TODO: private void DirectRollback(string txId, string operationUrl)

  //TODO: public void PrepareTransaction(string txId)

  //TODO: private void DirectPrepareTransaction(string txId, string operationUrl)

  //TODO: public IDatabaseCommands With(ICredentials credentialsForSession)

  //TODO: public ILowLevelBulkInsertOperation GetBulkInsertOperation(BulkInsertOptions options, IDatabaseChanges changes)

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

  public IDatabaseCommands forDatabase(String database) {
    if (Constants.SYSTEM_DATABASE.equals(database)) {
      return forSystemDatabase();
    }

    String databaseUrl = MultiDatabase.getRootDatabaseUrl(url);
    databaseUrl = url + "/databases/" + database;
    if (databaseUrl.equals(url)) {
      return this;
    }
    ServerClient client = new ServerClient(databaseUrl, convention, credentials, replicationInformerGetter, database, jsonRequestFactory, currentSessionId, conflictListeners);
    client.setOperationsHeaders(operationsHeaders);
    return client;
  }

  public IDatabaseCommands forSystemDatabase() {
    String databaseUrl = MultiDatabase.getRootDatabaseUrl(url);
    if (databaseUrl.equals(url)) {
      return this;
    }
    ServerClient client = new ServerClient(databaseUrl, convention, credentials, replicationInformerGetter, null, jsonRequestFactory, currentSessionId, conflictListeners);
    client.setOperationsHeaders(operationsHeaders);
    return client;
  }

  /**
   * @return the url
   */
  public String getUrl() {
    return url;
  }

  //TODO: public Operation DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale)

  //TODO: public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests)

  //TODO: public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch)

  //TODO: public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale)

  //TODO: public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, bool allowStale)

  //TODO: private Operation UpdateByIndexImpl(string indexName, IndexQuery queryToUpdate, bool allowStale, String requestData, String method)

  //TODO: public Operation DeleteByIndex(string indexName, IndexQuery queryToDelete)

  //TODO: public SuggestionQueryResult Suggest(string index, SuggestionQuery suggestionQuery)

  //TODO: public MultiLoadResult MoreLikeThis(MoreLikeThisQuery query)

  //TODO: public DatabaseStatistics GetStatistics()

  public Long nextIdentityFor(final String name) {
    return executeWithReplication(HttpMethods.POST, new Function1<String, Long>() {
      @Override
      public Long apply(String url) {
        return directNextIdentityFor(name, url);
      }
    });
  }

  protected Long directNextIdentityFor(String name, String operationUrl) {
    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/identity/next?name=" + UrlUtils.escapeDataString(name),
        HttpMethods.POST, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders));
    try {
      RavenJToken ravenJToken = jsonRequest.readResponseJson();
      return ravenJToken.value(Long.class, "Value");
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  @Override
  public String urlFor(String documentKey) {
    return url + "/docs/" + documentKey;
  }

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
        new CreateHttpJsonRequestParams(ServerClient.this, serverUrl + "/docs/" + key, HttpMethods.HEAD, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
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



  //TODO: public GetResponse[] MultiGet(GetRequest[] requests)

  //TODO: public IEnumerable<string> GetTerms(string index, string field, string fromValue, int pageSize)

  //TODO: public FacetResults GetFacets(string index, IndexQuery query, string facetSetupDoc, int start, int? pageSize)

  //TODO: public FacetResults GetFacets(string index, IndexQuery query, List<Facet> facets, int start, int? pageSize)

  //TODO: public RavenJObject Patch(string key, PatchRequest[] patches)

  //TODO: public RavenJObject Patch(string key, PatchRequest[] patches, bool ignoreMissing)

  //TODO: public RavenJObject Patch(string key, ScriptedPatchRequest patch)

  //TODO: public RavenJObject Patch(string key, ScriptedPatchRequest patch, bool ignoreMissing)

  //TODO: public RavenJObject Patch(string key, PatchRequest[] patches, Etag etag)

  //TODO: public RavenJObject Patch(string key, PatchRequest[] patchesToExisting, PatchRequest[] patchesToDefault, RavenJObject defaultMetadata)

  //TODO: public RavenJObject Patch(string key, ScriptedPatchRequest patch, Etag etag)

  //TODO: public RavenJObject Patch(string key, ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata)


  //FIXME: public IDisposable DisableAllCaching()

  /**
   * @return the profilingInformation
   */
  public ProfilingInformation getProfilingInformation() {
    return profilingInformation;
  }

  //TODO:public RavenJToken GetOperationStatus(long id)

  //TODO: public IDisposable Expect100Continue()


}
