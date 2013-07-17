package raven.client.connection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import raven.abstractions.basic.EventHandler;
import raven.abstractions.basic.Holder;
import raven.abstractions.basic.SharpEnum;
import raven.abstractions.closure.Action3;
import raven.abstractions.closure.Function0;
import raven.abstractions.closure.Function1;
import raven.abstractions.commands.ICommandData;
import raven.abstractions.commands.PatchCommandData;
import raven.abstractions.commands.ScriptedPatchCommandData;
import raven.abstractions.data.Attachment;
import raven.abstractions.data.BatchResult;
import raven.abstractions.data.Constants;
import raven.abstractions.data.DatabaseStatistics;
import raven.abstractions.data.Etag;
import raven.abstractions.data.Facet;
import raven.abstractions.data.FacetResults;
import raven.abstractions.data.GetRequest;
import raven.abstractions.data.GetResponse;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.data.IndexQuery;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.JsonDocumentMetadata;
import raven.abstractions.data.MoreLikeThisQuery;
import raven.abstractions.data.MultiLoadResult;
import raven.abstractions.data.PatchRequest;
import raven.abstractions.data.PatchResult;
import raven.abstractions.data.PutResult;
import raven.abstractions.data.QueryHeaderInformation;
import raven.abstractions.data.QueryResult;
import raven.abstractions.data.ScriptedPatchRequest;
import raven.abstractions.data.SuggestionQuery;
import raven.abstractions.data.SuggestionQueryResult;
import raven.abstractions.data.TransactionInformation;
import raven.abstractions.exceptions.ConcurrencyException;
import raven.abstractions.exceptions.DocumentDoesNotExistsException;
import raven.abstractions.exceptions.HttpOperationException;
import raven.abstractions.exceptions.ServerClientException;
import raven.abstractions.extensions.JsonExtensions;
import raven.abstractions.extensions.MetadataExtensions;
import raven.abstractions.indexing.IndexDefinition;
import raven.abstractions.indexing.TransformerDefinition;
import raven.abstractions.json.linq.JTokenType;
import raven.abstractions.json.linq.RavenJArray;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.abstractions.json.linq.RavenJValue;
import raven.client.connection.ReplicationInformer.FailoverStatusChangedEventArgs;
import raven.client.connection.profiling.ProfilingInformation;
import raven.client.document.DocumentConvention;
import raven.client.exceptions.ConflictException;
import raven.client.extensions.MultiDatabase;
import raven.client.indexes.IndexDefinitionBuilder;
import raven.client.listeners.IDocumentConflictListener;
import raven.client.utils.TimeSpan;
import raven.client.utils.UrlUtils;
import raven.imports.json.JsonConvert;

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

  private boolean expect100Continue = false;

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
      if (results.get(0) == null) {
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
      try {
        HttpResponse httpWebResponse = e.getHttpResponse();
        if (httpWebResponse.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
          return null;
        } else if (httpWebResponse.getStatusLine().getStatusCode() == HttpStatus.SC_CONFLICT) {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          try {
            IOUtils.copy(e.getHttpResponse().getEntity().getContent(), baos);

            RavenJObject conflictsDoc = (RavenJObject) RavenJObject.tryLoad(new ByteArrayInputStream(baos.toByteArray()));
            Etag etag = HttpExtensions.getEtagHeader(e.getHttpResponse());

            ConflictException concurrencyException = tryResolveConflictOrCreateConcurrencyException(key, conflictsDoc, etag);

            if (concurrencyException == null) {
              if (resolvingConflictRetries) {
                throw new IllegalStateException("Encountered another conflict after already resolving a conflict. Conflict resultion cannot recurse.");
              }

              resolvingConflictRetries = true;
              try {
                return directGet(serverUrl, key);
              } finally {
                resolvingConflictRetries = false;
              }
            }
            throw concurrencyException;

          } catch (IOException e1) {
            throw new ServerClientException(e1);
          }

        }
        throw new ServerClientException(e);
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
      }
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  private void handleReplicationStatusChanges(Map<String, String> headers, String primaryUrl, String currentUrl) {
    if (!primaryUrl.equalsIgnoreCase(currentUrl)) {
      String forceCheck = headers.get(Constants.RAVEN_FORCE_PRIMARY_SERVER_CHECK);
      boolean shouldForceCheck;
      if (StringUtils.isNotEmpty(forceCheck)) {
        shouldForceCheck = Boolean.valueOf(forceCheck);
        replicationInformer.forceCheck(primaryUrl, shouldForceCheck);
      }
    }
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
      try {
        if (e.getStatusCode() == HttpStatus.SC_CONFLICT) {
          throw throwConcurrencyException(e);
        }
        throw e;
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
      }
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
        try {
          HttpOperationException httpException = (HttpOperationException) e;
          if (httpException.getStatusCode() == HttpStatus.SC_CONFLICT) {
            throw throwConcurrencyException(httpException);
          }
        } finally {
          EntityUtils.consumeQuietly(((HttpOperationException)e).getHttpResponse().getEntity());
        }
      }
      throw new ServerClientException(e);
    }
  }

  protected void addTransactionInformation(RavenJObject metadata) {
    if (convention.isEnlistInDistributedTransactions() == false) {
      return ;
    }
    TransactionInformation transactionInformation = RavenTransactionAccessor.getTransactionInformation();
    if (transactionInformation == null) {
      return ;
    }
    metadata.add("Raven-Transaction-Information", new RavenJValue(String.format("%s, %s", transactionInformation.getId(), TimeSpan.formatString(transactionInformation.getTimeout()))));
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
      metadata.set("ETag", new RavenJValue(etag.toString()));
    }

    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + key, HttpMethods.POST, metadata, credentials, convention))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      jsonRequest.executeRequest();
    } catch (HttpOperationException e) {
      try {
        if (e.getStatusCode() != HttpStatus.SC_INTERNAL_SERVER_ERROR) {
          throw new ServerClientException(e);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          IOUtils.copy(e.getHttpResponse().getEntity().getContent(), baos);
        } catch (IOException e1) {
          throw new ServerClientException(e1);
        }
        throw new ServerClientException("Internal server error: " + baos.toString());
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
      }
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
      return SerializationHelper.deserializeAttachements(responseJson, false);

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
      try {
        if (e.getStatusCode() == HttpStatus.SC_CONFLICT) {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          try {
            IOUtils.copy(e.getHttpResponse().getEntity().getContent(), baos);
          } catch (IOException e1) {
            throw new ServerClientException(e1);
          } finally {
            EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
          }

          RavenJObject conflictsDoc = (RavenJObject) RavenJObject.tryLoad(new ByteArrayInputStream(baos.toByteArray()));
          List<String> conflictIds = conflictsDoc.value(RavenJArray.class, "Conflicts").values(String.class);

          ConflictException ex = new ConflictException("Conflict detected on " + key + ", conflict must be resolved before the attachement will be accessible", true);
          ex.setConflictedVersionIds(conflictIds.toArray(new String[0]));
          ex.setEtag(HttpExtensions.getEtagHeader(e.getHttpResponse()));
          throw ex;
        } else if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
          return null;
        }
        throw new ServerClientException(e);
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
      }

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
        new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + key, HttpMethods.DELETE, metadata, credentials, convention))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());
    try {
      jsonRequest.executeRequest();
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  public Collection<String> getIndexNames(final int start, final int pageSize) {
    return executeWithReplication(HttpMethods.GET, new Function1<String, Collection<String>>() {
      @Override
      public Collection<String> apply(String u) {
        return directGetIndexNames(start, pageSize, u);
      }
    });
  }

  public Collection<IndexDefinition> getIndexes(final int start, final int pageSize) {
    return executeWithReplication(HttpMethods.GET, new Function1<String, Collection<IndexDefinition>>() {
      @Override
      public Collection<IndexDefinition> apply(String operationUrl) {
        return directGetIndexes(start, pageSize, operationUrl);
      }
    });
  }

  protected Collection<IndexDefinition> directGetIndexes(int start, int pageSize, String operationUrl) {
    String url2 = RavenUrlExtensions.noCache(operationUrl + "/indexes/?start=" + start + "&pageSize=" + pageSize);
    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(this, url2, HttpMethods.GET, new RavenJObject(), credentials, convention));
    request.addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      RavenJArray json = (RavenJArray) request.readResponseJson();

      return JsonConvert.deserializeObject(json, IndexDefinition.class, "definition");

    } catch (Exception e) {
      throw new ServerClientException("Unable to get indexes", e);
    }
  }

  public Collection<TransformerDefinition> getTransformers(final int start, final int pageSize) {
    return executeWithReplication(HttpMethods.GET, new Function1<String, Collection<TransformerDefinition>>() {

      @Override
      public Collection<TransformerDefinition> apply(String operationUrl) {
        return directGetTransformers(operationUrl, start, pageSize);
      }
    });
  }

  protected Collection<TransformerDefinition> directGetTransformers(String operationUrl, int start, int pageSize) {
    String url2 = RavenUrlExtensions.noCache(operationUrl + "/transformers?start=" + start + "&pageSize=" + pageSize);
    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(this, url2, HttpMethods.GET, new RavenJObject(), credentials, convention));
    request.addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      RavenJToken result = request.readResponseJson();
      RavenJArray json = ((RavenJArray)result);
      return JsonConvert.deserializeObject(json, TransformerDefinition.class, "definition");
    } catch (IOException e) {
      throw new ServerClientException("unable to get transformers", e);
    }
  }

  public TransformerDefinition getTransformer(final String name) {
    ensureIsNotNullOrEmpty(name, "name");
    return executeWithReplication(HttpMethods.GET, new Function1<String, TransformerDefinition>() {
      @Override
      public TransformerDefinition apply(String u) {
        return directGetTransformer(name, u);
      }
    });
  }

  public void deleteTransformer(final String name) {
    ensureIsNotNullOrEmpty(name, "name");
    executeWithReplication(HttpMethods.DELETE, new Function1<String, Void>() {
      @Override
      public Void apply(String operationUrl) {
        directDeleteTransformer(name, operationUrl);
        return null;
      }
    });
  }
  private void directDeleteTransformer(final String name, String operationUrl) {
    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/transformers/" + name, HttpMethods.DELETE, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      request.executeRequest();
    } catch (IOException e) {
      throw new ServerClientException("Unable to delete transformer", e);
    }
  }

  private TransformerDefinition directGetTransformer(final String transformerName, final String operationUrl) {
    HttpJsonRequest httpJsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/transformers/" + transformerName, HttpMethods.GET, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      RavenJToken transformerDef;
      try {
        transformerDef = httpJsonRequest.readResponseJson();
      } catch (HttpOperationException e) {
        try {
          if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
            return null;
          }
          throw e;
        } finally {
          EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
        }
      }

      RavenJObject value = transformerDef.value(RavenJObject.class, "Transformer");
      return JsonExtensions.getDefaultObjectMapper().readValue(value.toString(), TransformerDefinition.class);
    } catch (IOException e){
      throw new ServerClientException("unable to get transformer:" + transformerName, e);
    }
  }

  @Override
  public void resetIndex(final String name) {
    executeWithReplication(HttpMethods.RESET, new Function1<String, Void>() {
      @Override
      public Void apply(String u) {
        directResetIndex(name, u);
        return null;
      }
    });
  }

  private void directResetIndex(String name, String operationUrl) {
    HttpJsonRequest httpJsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/indexes/" + name, HttpMethods.RESET, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());


    try {
      httpJsonRequest.readResponseJson();
    } catch (IOException e) {
      throw new ServerClientException(e);
    }
  }

  private Collection<String> directGetIndexNames(int start, int pageSize, String operationUrl) {
    try {
      HttpJsonRequest httpJsonRequest = jsonRequestFactory.createHttpJsonRequest(
          new CreateHttpJsonRequestParams(this, operationUrl + "/indexes/?namesOnly=true&start=" + start + "&pageSize=" + pageSize, HttpMethods.GET, new RavenJObject(), credentials, convention)
          .addOperationHeaders(operationsHeaders))
          .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());


      RavenJArray responseJson = (RavenJArray) httpJsonRequest.readResponseJson();
      return responseJson.values(String.class);
    } catch (IOException e) {
      throw new ServerClientException(e);
    }
  }

  public IndexDefinition getIndex(final String name) {
    ensureIsNotNullOrEmpty(name, "name");
    return executeWithReplication(HttpMethods.GET, new Function1<String, IndexDefinition>() {
      @Override
      public IndexDefinition apply(String u) {
        return directGetIndex(name, u);
      }
    });
  }


  private IndexDefinition directGetIndex(String indexName, String operationUrl) {
    HttpJsonRequest httpJsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/indexes/" + indexName + "?definition=yes", HttpMethods.GET, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    RavenJToken indexDef;
    try {
      indexDef = httpJsonRequest.readResponseJson();
    } catch (HttpOperationException e) {
      try {
        if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
          return null;
        }
        throw e;
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
      }
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
    RavenJObject value = indexDef.value(RavenJObject.class, "Index");
    try {
      return JsonExtensions.getDefaultObjectMapper().readValue(value.toString(), IndexDefinition.class);
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

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
      try {
        if (HttpStatus.SC_CONFLICT == e.getStatusCode()) {
          throw throwConcurrencyException(e);
        } else {
          throw e;
        }
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
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

    return new ConcurrencyException(ravenJToken.value(Etag.class, "ExpectedETag"), ravenJToken.value(Etag.class, "ActualETag"), ravenJToken.value(String.class, "Error"), e);

  }

  public String putIndex(final String name, final IndexDefinition definition) {
    return putIndex(name, definition, false);
  }

  public String putTransformer(final String name, final TransformerDefinition indexDef) {
    ensureIsNotNullOrEmpty(name, "name");
    return executeWithReplication(HttpMethods.PUT, new Function1<String, String>() {
      @Override
      public String apply(String operationUrl) {
        return directPutTransformer(name, operationUrl, indexDef);
      }
    });
  }

  public String putIndex(final String name, final IndexDefinition definition, final boolean overwrite) {
    ensureIsNotNullOrEmpty(name, "name");
    return executeWithReplication(HttpMethods.PUT, new Function1<String, String>() {
      @Override
      public String apply(String operationUrl) {
        return directPutIndex(name, operationUrl, overwrite, definition);
      }
    });
  }

  public String directPutTransformer(String name, String operationUrl, TransformerDefinition definition) {
    String requestUri = operationUrl + "/transformers/" + name;

    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, requestUri, HttpMethods.PUT, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      request.write(JsonConvert.serializeObject(definition));


      RavenJObject responseJson = (RavenJObject) request.readResponseJson();
      return responseJson.value(String.class, "Transformer");
    } catch (HttpOperationException e) {
      try {
        throw new ServerClientException("unable to put transformer", e);
        /*TODO: dead code - reported - RavenDB-1216
      var httpWebResponse = e.Response as HttpWebResponse;
      if (httpWebResponse == null || httpWebResponse.StatusCode != HttpStatusCode.NotFound)
        throw;

      if (httpWebResponse.StatusCode == HttpStatusCode.BadRequest)
      {
        var error = e.TryReadErrorResponseObject(
            new { Error = "", Message = "" });

        if (error == null)
        {
          throw;
        }

        var compilationException = new TransformCompilationException(error.Message);

        throw compilationException;
      }

      throw;*/
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
      }
    } catch (IOException e) {
      throw new ServerClientException(e);
    }
  }

  public String directPutIndex(String name, String operationUrl, boolean overwrite, IndexDefinition definition) {
    String requestUri = operationUrl + "/indexes/" + name;

    HttpJsonRequest checkIndexExists = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, requestUri, HttpMethods.HEAD,new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      // If the index doesn't exist this will throw a NotFound exception and continue with a PUT request
      checkIndexExists.executeRequest();
      if (!overwrite) {
        throw new IllegalStateException("Cannot put index: " + name + ", index already exists");
      }
    } catch (HttpOperationException e) {
      try {
        if (e.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
          throw new ServerClientException(e);
        }
        /*
         * TODO dead code - reported - RavenDB-1216
         *   var httpWebResponse = e.Response as HttpWebResponse;
              if (httpWebResponse == null || httpWebResponse.StatusCode != HttpStatusCode.NotFound)
                  throw;

              if (httpWebResponse.StatusCode == HttpStatusCode.BadRequest)
              {
                  var error = e.TryReadErrorResponseObject(
                      new {Error = "", Message = "", IndexDefinitionProperty = "", ProblematicText = ""});

                  if (error == null)
                  {
                      throw;
                  }

                  var compilationException = new IndexCompilationException(error.Message)
                  {
                      IndexDefinitionProperty = error.IndexDefinitionProperty,
                      ProblematicText = error.ProblematicText
                  };

                  throw compilationException;
              }
         */
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
      }
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, requestUri, HttpMethods.PUT, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      request.write(JsonConvert.serializeObject(definition)); //we don't use default converters

      try {
        RavenJToken responseJson = request.readResponseJson();
        return responseJson.value(String.class, "Index");
      } catch (HttpOperationException e) {
        throw new ServerClientException(e);
        /*TODO: dead code - reported - RavenDB-1216
        var httpWebResponse = e.Response as HttpWebResponse;
        if (httpWebResponse == null || httpWebResponse.StatusCode != HttpStatusCode.NotFound)
            throw;

        if (httpWebResponse.StatusCode == HttpStatusCode.BadRequest)
        {
            var error = e.TryReadErrorResponseObject(
                new {Error = "", Message = "", IndexDefinitionProperty = "", ProblematicText = ""});

            if (error == null)
            {
                throw;
            }

            var compilationException = new IndexCompilationException(error.Message)
            {
                IndexDefinitionProperty = error.IndexDefinitionProperty,
                ProblematicText = error.ProblematicText
            };

            throw compilationException;
        }

        throw;
    }*/
      }
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  public String putIndex(String name, IndexDefinitionBuilder indexDef) {
    return putIndex(name, indexDef.toIndexDefinition(convention));
  }

  public String putIndex(String name, IndexDefinitionBuilder indexDef, boolean overwrite) {
    return putIndex(name, indexDef.toIndexDefinition(convention), overwrite);
  }

  public QueryResult query(String index, IndexQuery query, String[] includes) {
    return query(index, query, includes, false, false);
  }

  public QueryResult query(String index, IndexQuery query, String[] includes, boolean metadataOnly) {
    return query(index, query, includes, metadataOnly, false);
  }

  public QueryResult query(final String index, final IndexQuery query, final String[] includes, final boolean metadataOnly, final boolean indexEntriesOnly) {
    ensureIsNotNullOrEmpty(index, "index");
    return executeWithReplication(HttpMethods.GET, new Function1<String, QueryResult>() {
      @Override
      public QueryResult apply(String u) {
        return directQuery(index, query, u, includes, metadataOnly, indexEntriesOnly);
      }
    });
  }

  public Iterator<RavenJObject> streamQuery(String index, IndexQuery query, Holder<QueryHeaderInformation> queryHeaderInfo) {
    ensureIsNotNullOrEmpty(index, "index");
    String path = query.getIndexQueryUrl(url, index, "streams/query", false);
    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, path,  HttpMethods.GET, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, url, replicationInformer,
            convention.getFailoverBehavior(),
            new HandleReplicationStatusChangesCallback());

    HttpResponse webResponse = request.rawExecuteRequest();

    QueryHeaderInformation queryHeaderInformation = new QueryHeaderInformation();
    Map<String, String> headers = HttpJsonRequest.extractHeaders(webResponse.getAllHeaders());
    queryHeaderInformation.setIndex(headers.get("Raven-Index"));
    //TODO: queryHeaderInformation.setIndexTimestamp() IndexTimestamp = DateTime.ParseExact(webResponse.Headers["Raven-Index-Timestamp"], Default.DateTimeFormatsToRead,
    //CultureInfo.InvariantCulture, DateTimeStyles.None),

    queryHeaderInformation.setIndexEtag(Etag.parse(headers.get("Raven-Index-Etag")));
    queryHeaderInformation.setResultEtag(Etag.parse(headers.get("Raven-Result-Etag")));
    queryHeaderInformation.setStable(Boolean.valueOf(headers.get("Raven-Is-Stale")));
    queryHeaderInformation.setTotalResults(Integer.valueOf(headers.get("Raven-Total-Results")));

    queryHeaderInfo.value = queryHeaderInformation;

    return yieldStreamResults(webResponse);
  }

  public Iterator<RavenJObject> streamDocs(Etag fromEtag, String startsWith, String matches, int start, int pageSize) {
    if (fromEtag != null && startsWith != null)
      throw new IllegalArgumentException("Either fromEtag or startsWith must be null, you can't specify both");

    StringBuilder sb = new StringBuilder(url).append("/streams/docs?");

    if (fromEtag != null) {
      sb.append("etag=")
      .append(fromEtag)
      .append("&");
    } else {
      if (startsWith != null) {
        sb.append("startsWith=").append(UrlUtils.escapeDataString(startsWith)).append("&");
      }
      if(matches != null) {
        sb.append("matches=").append(UrlUtils.escapeDataString(matches)).append("&");
      }
    }
    if (start != 0) {
      sb.append("start=").append(start).append("&");
    }
    if (pageSize != Integer.MAX_VALUE) {
      sb.append("pageSize=").append(pageSize).append("&");
    }


    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, sb.toString(), HttpMethods.GET, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, url, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());
    HttpResponse webResponse = request.rawExecuteRequest();
    return yieldStreamResults(webResponse);
  }

  private static Iterator<RavenJObject> yieldStreamResults(final HttpResponse webResponse) {
    HttpEntity httpEntity = webResponse.getEntity();
    try {
      InputStream stream = httpEntity.getContent();
      JsonParser jsonParser = JsonExtensions.getDefaultJsonFactory().createJsonParser(stream);
      if (jsonParser.nextToken() == null || jsonParser.getCurrentToken() != JsonToken.START_OBJECT) {
        throw new IllegalStateException("Unexpected data at start of stream");
      }
      if (jsonParser.nextToken() == null || jsonParser.getCurrentToken() != JsonToken.FIELD_NAME || !"Results".equals(jsonParser.getText())) {
        throw new IllegalStateException("Unexpected data at stream 'Results' property name");
      }
      if (jsonParser.nextToken() == null || jsonParser.getCurrentToken() != JsonToken.START_ARRAY) {
        throw new IllegalStateException("Unexpected data at 'Results', could not find start results array");
      }

      return new RavenJObjectIterator(httpEntity, jsonParser);
    } catch (IOException e) {
      throw new ServerClientException(e);
    }

  }

  private QueryResult directQuery(final String index, final IndexQuery query, final String operationUrl, final String[] includes, final boolean metadataOnly, final boolean includeEntries) {
    String path = query.getIndexQueryUrl(operationUrl, index, "indexes");
    if (metadataOnly)
      path += "&metadata-only=true";
    if (includeEntries)
      path += "&debug=entries";
    if (includes != null && includes.length > 0) {
      for (String include: includes) {
        path += "&include=" + include;
      }
    }
    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, path, HttpMethods.GET, new RavenJObject(), credentials, convention)
        .setAvoidCachingRequest(query.isDisableCaching())
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer,
            convention.getFailoverBehavior(),
            new HandleReplicationStatusChangesCallback());

    RavenJObject json;
    try {
      json = (RavenJObject)request.readResponseJson();
    } catch (HttpOperationException e) {
      try {
        if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          IOUtils.copy(e.getHttpResponse().getEntity().getContent(), baos);
          String text = new String(baos.toByteArray());
          if (text.contains("maxQueryString")) {
            throw new IllegalStateException(text, e);
          }
          throw new IllegalStateException("There is no index named: " + index);
        }

      } catch (IllegalStateException | IOException ee) {
        throw new ServerClientException(ee);
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
      }
      throw e;
    } catch (Exception e) {
      throw new ServerClientException(e);
    }

    QueryResult directQuery = SerializationHelper.toQueryResult(json, HttpExtensions.getEtagHeader(request), request.getResponseHeaders().get("Temp-Request-Time"));
    List<RavenJObject> docsResults = new ArrayList<>();
    docsResults.addAll(directQuery.getResults());
    docsResults.addAll(directQuery.getIncludes());
    return retryOperationBecauseOfConflict(docsResults, directQuery, new Function0<QueryResult>() {
      @Override
      public QueryResult apply() {
        return directQuery(index, query, operationUrl, includes, metadataOnly, includeEntries);
      }
    });
  }

  public void deleteIndex(final String name) {
    ensureIsNotNullOrEmpty(name, "name");
    executeWithReplication(HttpMethods.DELETE, new Function1<String, Void>() {
      @Override
      public Void apply(String operationUrl) {
        directDeleteIndex(name, operationUrl);
        return null;
      }
    });
  }

  private void directDeleteIndex(String name, String operationUrl) {
    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/indexes/" + name, HttpMethods.DELETE, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      request.executeRequest();
    } catch (Exception e) {
      throw new ServerClientException("Unable to delete index", e);
    }
  }



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


  protected MultiLoadResult directGet(final String[] ids, final String operationUrl, final String[] includes, final String transformer, final Map<String, RavenJToken> queryInputs, final boolean metadataOnly) {

    try {
      String path = operationUrl + "/queries/?";

      if (metadataOnly)
        path += "&metadata-only=true";
      if (includes != null && includes.length > 0) {
        List<String> tokens = new ArrayList<>();
        for (String include: includes) {
          tokens.add("include=" + include);
        }
        path += StringUtils.join(tokens, "&");
      }
      if (StringUtils.isNotEmpty(transformer)) {
        path += "&transformer=" + transformer;
      }

      if (queryInputs != null) {
        for (Entry<String, RavenJToken> queryInput: queryInputs.entrySet()) {
          path += String.format("&qp-%s=%s", queryInput.getKey(), queryInput.getValue());
        }
      }

      RavenJObject metadata = new RavenJObject();
      addTransactionInformation(metadata);
      Set<String> uniqueIds = new HashSet<>(Arrays.asList(ids));
      // if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
      // we are fine with that, requests to load that many items are probably going to be rare
      HttpJsonRequest request;

      int uniqueIdsSum = 0;
      for (String id: ids) {
        uniqueIdsSum += id.length();
      }

      if (uniqueIdsSum < 1024) {
        for (String uniqueId: uniqueIds) {
          path += "&id=" + UrlUtils.escapeDataString(uniqueId);
        }
        request = jsonRequestFactory.createHttpJsonRequest(
            new CreateHttpJsonRequestParams(this, path, HttpMethods.GET, metadata, credentials, convention)
            .addOperationHeaders(operationsHeaders))
            .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

      } else {
        request = jsonRequestFactory.createHttpJsonRequest(
            new CreateHttpJsonRequestParams(this, path, HttpMethods.POST, metadata, credentials, convention)
            .addOperationHeaders(operationsHeaders))
            .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());
        request.write(RavenJArray.fromObject(uniqueIds).toString());
      }

      RavenJObject result = (RavenJObject)request.readResponseJson();

      Collection<RavenJObject> results = result.value(RavenJArray.class, "Results").values(RavenJObject.class);
      MultiLoadResult multiLoadResult = new MultiLoadResult();
      multiLoadResult.setIncludes(new ArrayList<RavenJObject>(result.value(RavenJArray.class, "Includes").values(RavenJObject.class)));

      if (StringUtils.isEmpty(transformer)) {
        List<RavenJObject> values = new ArrayList<>();
        outerFor:
          for (String id : ids) {

            for (RavenJObject jObject : results) {
              if (StringUtils.equals(id, jObject.get("@metadata").value(String.class, "@id"))) {
                values.add(jObject);
                continue outerFor;
              }
            }
            values.add(null);
          }
        multiLoadResult.setResults(values);
      } else {
        multiLoadResult.setResults(new ArrayList<RavenJObject>(results));
      }

      List<RavenJObject> docResults = new ArrayList<>();
      docResults.addAll(multiLoadResult.getResults());
      docResults.addAll(multiLoadResult.getIncludes());

      return retryOperationBecauseOfConflict(docResults, multiLoadResult, new Function0<MultiLoadResult>() {
        @Override
        public MultiLoadResult apply() {
          return directGet(ids, operationUrl, includes, transformer, queryInputs, metadataOnly);
        }
      });
    } catch (IOException e) {
      throw new ServerClientException(e);
    }
  }


  private <T> T retryOperationBecauseOfConflict(List<RavenJObject> docResults, T currentResult, Function0<T> nextTry) {

    boolean requiresRetry = false;
    for (RavenJObject docResult: docResults) {
      requiresRetry |= assertNonConflictedDocumentAndCheckIfNeedToReload(docResult);
    }
    if (!requiresRetry) {
      return currentResult;
    }

    if (resolvingConflictRetries) {
      throw new IllegalStateException("Encountered another conflict after already resolving a conflict. Conflict resultion cannot recurse.");
    }
    resolvingConflictRetries = true;
    try {
      return nextTry.apply();
    } finally {
      resolvingConflictRetries = false;
    }
  }
  private boolean assertNonConflictedDocumentAndCheckIfNeedToReload(RavenJObject docResult) {
    if (docResult == null) {
      return false;
    }
    RavenJToken metadata = docResult.get(Constants.METADATA);
    if (metadata == null) {
      return false;
    }

    if (metadata.value(Integer.TYPE, "@Http-Status-Code") == 409) {
      ConflictException concurrencyException = tryResolveConflictOrCreateConcurrencyException(metadata.value(String.class, "@id"), docResult, HttpExtensions.etagHeaderToEtag(metadata.value(String.class, "@etag")));
      if (concurrencyException == null) {
        return true;
      }
      throw concurrencyException;
    }
    return false;
  }



  public BatchResult[] batch(final List<ICommandData> commandDatas) {
    return executeWithReplication(HttpMethods.POST, new Function1<String, BatchResult[]>() {
      @Override
      public BatchResult[] apply(String u) {
        return directBatch(commandDatas, u);
      }
    });
  }

  private BatchResult[] directBatch(List<ICommandData> commandDatas, String operationUrl) {
    RavenJObject metadata = new RavenJObject();
    addTransactionInformation(metadata);
    HttpJsonRequest req = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/bulk_docs", HttpMethods.POST, metadata, credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());


    RavenJArray jArray = new RavenJArray();
    for (ICommandData command: commandDatas) {
      jArray.add(command.toJson());
    }
    try {
      req.write(jArray.toString());

      RavenJArray response;
      try {
        response = (RavenJArray)req.readResponseJson();
      } catch (HttpOperationException e) {
        try {
          if (e.getStatusCode() != HttpStatus.SC_CONFLICT) {
            throw e;
          }
          throw throwConcurrencyException(e);
        } finally {
          EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
        }
      }
      return JsonExtensions.getDefaultObjectMapper().readValue(response.toString(), BatchResult[].class);
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  /**
   * Commits the specified tx id.
   * @param txId
   */
  public void commit(final String txId) {
    executeWithReplication(HttpMethods.POST, new Function1<String, Void>() {
      @Override
      public Void apply(String u) {
        directCommit(txId, u);
        return null;
      }
    });
  }

  private void directCommit(String txId, String operationUrl) {
    HttpJsonRequest httpJsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/transaction/commit?tx=" + txId, HttpMethods.POST, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      httpJsonRequest.readResponseJson();
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  /**
   * Rollbacks the specified tx id.
   * @param txId
   */
  public void rollback(final String txId) {
    executeWithReplication(HttpMethods.POST, new Function1<String, Void>() {
      @Override
      public Void apply(String u) {
        directRollback(txId, u);
        return null;
      }
    });
  }

  private void directRollback(String txId, String operationUrl) {
    HttpJsonRequest httpJsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/transaction/rollback?tx=" + txId, HttpMethods.POST, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      httpJsonRequest.readResponseJson();
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  /**
   * Prepares the transaction on the server.
   * @param txId
   */
  public void prepareTransaction(final String txId) {
    executeWithReplication(HttpMethods.POST, new Function1<String, Void>() {
      @Override
      public Void apply(String u) {
        directPrepareTransaction(txId, u);
        return null;
      }
    });
  }

  private void directPrepareTransaction(String txId, String operationUrl) {
    HttpJsonRequest httpJsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, operationUrl + "/transaction/prepare?tx=" + txId, HttpMethods.POST, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      httpJsonRequest.readResponseJson();
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  /**
   * Returns a new {@link IDatabaseCommands} using the specified credentials
   * @param credentialsForSession
   * @return
   */
  public IDatabaseCommands with(Credentials credentialsForSession) {
    return new ServerClient(url, convention, credentialsForSession, replicationInformerGetter, databaseName, jsonRequestFactory, currentSessionId, conflictListeners);
  }

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

  /**
   *  Perform a set based deletes using the specified index.
   * @param indexName
   * @param queryToDelete
   * @param allowStale
   * @return
   */
  public Operation deleteByIndex(final String indexName, final IndexQuery queryToDelete, final boolean allowStale) {
    return executeWithReplication(HttpMethods.DELETE, new Function1<String, Operation>() {
      @Override
      public Operation apply(String operationUrl) {
        return directDeleteByIndex(operationUrl, indexName, queryToDelete, allowStale);
      }
    });
  }

  private Operation directDeleteByIndex(String operationUrl, String indexName, IndexQuery queryToDelete, boolean allowStale) {
    String path = queryToDelete.getIndexQueryUrl(operationUrl, indexName, "bulk_docs") + "&allowStale=" + allowStale;
    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, path, HttpMethods.DELETE, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());
    RavenJToken jsonResponse;
    try {
      jsonResponse = request.readResponseJson();
    } catch (HttpOperationException e) {
      try {
        if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
          throw new IllegalStateException("There is no index named: " + indexName);
        }
        throw new ServerClientException(e);
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
      }
    } catch (Exception e) {
      throw new ServerClientException(e);
    }

    return new Operation(this, jsonResponse.value(Long.TYPE,"OperationId"));
  }

  /**
   *  Perform a set based update using the specified index, not allowing the operation
   *  if the index is stale
   * @param indexName
   * @param queryToUpdate
   * @param patchRequests
   * @return
   */
  public Operation updateByIndex(String indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests) {
    return updateByIndex(indexName, queryToUpdate, patchRequests, false);
  }

  /**
   * Perform a set based update using the specified index, not allowing the operation
   *  if the index is stale
   * @param indexName
   * @param queryToUpdate
   * @param patch
   * @return
   */
  public Operation updateByIndex(String indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch) {
    return updateByIndex(indexName, queryToUpdate, patch, false);
  }

  /**
   * Perform a set based update using the specified index.
   * @param indexName
   * @param queryToUpdate
   * @param patchRequests
   * @param allowStale
   * @return
   */
  public Operation updateByIndex(String indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, boolean allowStale) {
    RavenJArray array = new RavenJArray();
    for (PatchRequest request: patchRequests) {
      array.add(request.toJson());
    }

    String requestData = array.toString();
    return updateByIndexImpl(indexName, queryToUpdate, allowStale, requestData, HttpMethods.PATCH);
  }

  /**
   * Perform a set based update using the specified index.
   * @param indexName
   * @param queryToUpdate
   * @param patch
   * @param allowStale
   * @return
   */
  public Operation updateByIndex(String indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, boolean allowStale) {
    String requestData = RavenJObject.fromObject(patch).toString();
    return updateByIndexImpl(indexName, queryToUpdate, allowStale, requestData, HttpMethods.EVAL);
  }

  private Operation updateByIndexImpl(final String indexName, final IndexQuery queryToUpdate, final boolean allowStale, final String requestData, final HttpMethods method) {
    return executeWithReplication(method, new Function1<String, Operation>() {
      @Override
      public Operation apply(String operationUrl) {
        return directUpdateByIndexImpl(operationUrl, indexName, queryToUpdate, allowStale, requestData, method);
      }
    });
  }

  protected Operation directUpdateByIndexImpl(String operationUrl, String indexName, IndexQuery queryToUpdate, boolean allowStale, String requestData, HttpMethods method) {
    String path = queryToUpdate.getIndexQueryUrl(operationUrl, indexName, "bulk_docs") + "&allowStale=" + allowStale;
    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, path, method, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    RavenJToken jsonResponse;
    try {
      request.write(requestData);
      jsonResponse = request.readResponseJson();
    } catch (HttpOperationException e) {
      try {
        if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
          throw new IllegalStateException("There is no index named: " + indexName);
        }
        throw new ServerClientException(e);
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
      }
    } catch (Exception e) {
      throw new ServerClientException(e);
    }

    return new Operation(this, jsonResponse.value(Long.TYPE, "OperationId"));
  }

  /**
   * Perform a set based deletes using the specified index, not allowing the operation
   *  if the index is stale
   * @param indexName
   * @param queryToDelete
   * @return
   */
  public Operation deleteByIndex(String indexName, IndexQuery queryToDelete) {
    return deleteByIndex(indexName, queryToDelete, false);
  }

  public SuggestionQueryResult suggest(final String index, final SuggestionQuery suggestionQuery) {
    if (suggestionQuery == null) {
      throw new IllegalArgumentException("suggestionQuery");
    }
    return executeWithReplication(HttpMethods.GET, new Function1<String, SuggestionQueryResult>() {

      @Override
      public SuggestionQueryResult apply(String operationUrl) {
        return directSuggest(index, suggestionQuery, operationUrl);
      }
    });
  }

  public MultiLoadResult moreLikeThis(MoreLikeThisQuery query) {
    RavenJToken token = executeGetRequest(query.getRequestUri());

    MultiLoadResult multiLoadResult = new MultiLoadResult();
    multiLoadResult.setIncludes(new ArrayList<RavenJObject>(token.value(RavenJArray.class, "Includes").values(RavenJObject.class)));
    multiLoadResult.setResults(new ArrayList<RavenJObject>(token.value(RavenJArray.class, "Results").values(RavenJObject.class)));

    return multiLoadResult;
  }

  protected SuggestionQueryResult directSuggest(String index, SuggestionQuery suggestionQuery, String operationUrl) {
    String requestUri = operationUrl + String.format("/suggest/%s?term=%s&field=%s&max=%d&distance=%s&accuracy=%.4f&popularity=%s",
        UrlUtils.escapeDataString(index),
        UrlUtils.escapeDataString(suggestionQuery.getTerm()),
        UrlUtils.escapeDataString(suggestionQuery.getField()),
        suggestionQuery.getMaxSuggestions(),
        UrlUtils.escapeDataString(SharpEnum.value(suggestionQuery.getDistance())),
        suggestionQuery.getAccuracy(),
        suggestionQuery.isPopularity());

    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, requestUri, HttpMethods.GET, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      RavenJObject json = (RavenJObject)request.readResponseJson();

      List<String> suggestions = new ArrayList<>();

      SuggestionQueryResult result = new SuggestionQueryResult();
      RavenJArray array = (RavenJArray) json.get("Suggestions");
      for (RavenJToken token: array) {
        suggestions.add(token.value(String.class));
      }
      result.setSuggestions(suggestions.toArray(new String[0]));
      return result;
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  public DatabaseStatistics getStatistics() {
    HttpJsonRequest httpJsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, url + "/stats", HttpMethods.GET, new RavenJObject(), credentials, convention));

    try {
      RavenJObject jo = (RavenJObject)httpJsonRequest.readResponseJson();
      return JsonExtensions.getDefaultObjectMapper().readValue(jo.toString(), DatabaseStatistics.class);
    } catch (IOException e) {
      throw new ServerClientException(e);
    }
  }

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
      jsonRequest.executeRequest();
      return SerializationHelper.deserializeJsonDocumentMetadata(key, jsonRequest.getResponseHeaders(), jsonRequest.getResponseStatusCode());
    } catch (HttpOperationException e) {
      try {
        if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
          return null;
        } else if (e.getStatusCode() == HttpStatus.SC_CONFLICT) {
          ConflictException conflictException = new ConflictException("Conflict detected on " + key +
              ", conflict must be resolved before the document will be accessible. Cannot get the conflicts ids because" +
              " a HEAD request was performed. A GET request will provide more information, and if you have a document conflict listener, will automatically resolve the conflict", true);
          conflictException.setEtag(HttpExtensions.getEtagHeader(e.getHttpResponse()));

          throw conflictException;
        }
        throw new ServerClientException(e);
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
      }
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  /**
   * Perform a single POST request containing multiple nested GET requests
   * @param requests
   * @return
   */
  public GetResponse[] multiGet(final GetRequest[] requests) {
    for (GetRequest getRequest: requests) {
      getRequest.getHeaders().put("Raven-Client-Version", HttpJsonRequest.clientVersion);
    }
    return executeWithReplication(HttpMethods.GET, new Function1<String, GetResponse[]>() {
      @Override
      public GetResponse[] apply(String operationUrl) {
        return directMultiGet(operationUrl, requests);
      }
    });
  }

  protected GetResponse[] directMultiGet(String operationUrl, GetRequest[] requests) {
    try {
      MultiGetOperation multiGetOperation = new MultiGetOperation(this, convention, operationUrl, requests);

      HttpJsonRequest httpJsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(this, multiGetOperation.getRequestUri(),
          HttpMethods.POST, new RavenJObject(), credentials, convention));

      GetRequest[] requestsForServer =
          multiGetOperation.preparingForCachingRequest(jsonRequestFactory);

      String postedData = JsonExtensions.getDefaultObjectMapper().writeValueAsString(requestsForServer);

      if (multiGetOperation.canFullyCache(jsonRequestFactory, httpJsonRequest, postedData)) {
        return multiGetOperation.handleCachingResponse(new GetResponse[requests.length],
            jsonRequestFactory);
      }

      httpJsonRequest.write(postedData);
      RavenJArray results = (RavenJArray)httpJsonRequest.readResponseJson();
      GetResponse[] responses = results.values(GetResponse.class).toArray(new GetResponse[0]);


      // 1.0 servers return result as string, not as an object, need to convert here
      for (GetResponse response: responses) {
        if (response != null && response.getResult() != null && response.getResult().getType() == JTokenType.STRING) {
          String value = response.getResult().value(String.class);
          if (StringUtils.isNotEmpty(value)) {
            response.setResult(RavenJObject.parse(value));
          } else {
            response.setResult(RavenJValue.getNull());
          }
        }
      }

      return multiGetOperation.handleCachingResponse(responses, jsonRequestFactory);
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  public List<String> getTerms(final String index, final String field, final String fromValue, final int pageSize) {
    return executeWithReplication(HttpMethods.GET, new Function1<String, List<String>>() {
      @Override
      public List<String> apply(String operationUrl) {
        return directGetTerms(operationUrl, index, field, fromValue, pageSize);
      }
    });
  }

  protected List<String> directGetTerms(String operationUrl, String index, String field, String fromValue, int pageSize) {
    String requestUri = operationUrl + String.format("/terms/%s?field=%s&pageSize=%d&fromValue=%s",
        UrlUtils.escapeDataString(index),
        UrlUtils.escapeDataString(field),
        pageSize,
        UrlUtils.escapeDataString(fromValue != null ? fromValue : ""));

    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, requestUri, HttpMethods.GET, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      return request.readResponseJson().values(String.class);
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  /**
   * Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
   * @param index
   * @param query
   * @param facetSetupDoc
   * @param start
   * @param pageSize
   * @return
   */
  public FacetResults getFacets(final String index, final IndexQuery query, final String facetSetupDoc, final int start, final Integer pageSize) {
    return executeWithReplication(HttpMethods.GET, new Function1<String, FacetResults>() {

      @Override
      public FacetResults apply(String operationUrl) {
        return directGetFacets(operationUrl, index, query, facetSetupDoc, start, pageSize);
      }
    });
  }

  protected FacetResults directGetFacets(String operationUrl, String index, IndexQuery query, String facetSetupDoc, int start, Integer pageSize) {
    String requestUri = operationUrl + String.format("/facets/%s?facetDoc=%s&%s&facetStart=%d&facetPageSize=%d",
        UrlUtils.escapeDataString(index),
        UrlUtils.escapeDataString(facetSetupDoc),
        query.getMinimalQueryString(),
        start,
        pageSize);

    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, requestUri, HttpMethods.GET, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());


    try {
      RavenJObject json = (RavenJObject)request.readResponseJson();
      return JsonExtensions.getDefaultObjectMapper().readValue(json.toString(), FacetResults.class);
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  /**
   * Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
   * @param index
   * @param query
   * @param facets
   * @param start
   * @param pageSize
   * @return
   */
  public FacetResults getFacets(final String index, final IndexQuery query, final List<Facet> facets, final int start, final Integer pageSize) {
    final String facetsJson = JsonConvert.serializeObject(facets);
    final HttpMethods method = facetsJson.length() > 1024 ? HttpMethods.POST : HttpMethods.GET;
    return executeWithReplication(method, new Function1<String, FacetResults>() {
      @Override
      public FacetResults apply(String operationUrl) {
        return directGetFacets(operationUrl, index, query, facetsJson, start, pageSize, method);
      }
    });
  }

  protected FacetResults directGetFacets(String operationUrl, String index, IndexQuery query, String facetsJson, int start, Integer pageSize, HttpMethods method) {
    String requestUri = operationUrl + String.format("/facets/%s?%s&facetStart=%d&facetPageSize=%d",
        UrlUtils.escapeDataString(index),
        query.getMinimalQueryString(),
        start,
        pageSize);

    if(method == HttpMethods.GET) {
      requestUri += "&facets=" + UrlUtils.escapeDataString(facetsJson);
    }

    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, requestUri, method, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders))
        .addReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.getFailoverBehavior(), new HandleReplicationStatusChangesCallback());

    try {
      if (method != HttpMethods.GET)
        request.write(facetsJson);

      RavenJObject json = (RavenJObject)request.readResponseJson();
      return JsonExtensions.getDefaultObjectMapper().readValue(json.toString(), FacetResults.class);
    } catch (Exception e) {
      throw new ServerClientException(e);
    }
  }

  /**
   * Sends a patch request for a specific document, ignoring the document's Etag
   * @param key
   * @param patches
   * @return
   */
  public RavenJObject patch(String key, PatchRequest[] patches) {
    return patch(key, patches, null);
  }

  /**
   * Sends a patch request for a specific document, ignoring the document's Etag
   * @param key
   * @param patches
   * @param ignoreMissing
   * @return
   */
  public RavenJObject patch(String key, PatchRequest[] patches, boolean ignoreMissing) {
    PatchCommandData command = new PatchCommandData();
    command.setKey(key);
    command.setPatches(patches);

    BatchResult[] batchResults = batch(Arrays.<ICommandData> asList(command));
    if (!ignoreMissing && batchResults[0].getPatchResult() != null && batchResults[0].getPatchResult() == PatchResult.DOCUMENT_DOES_NOT_EXISTS) {
      throw new DocumentDoesNotExistsException("Document with key " + key + " does not exist.");
    }
    return batchResults[0].getAdditionalData();
  }

  /**
   * Sends a patch request for a specific document, ignoring the document's Etag
   * @param key
   * @param patch
   * @return
   */
  public RavenJObject patch(String key, ScriptedPatchRequest patch) {
    return patch(key, patch, null);
  }

  /**
   * Sends a patch request for a specific document, ignoring the document's Etag
   * @param key
   * @param patch
   * @param ignoreMissing
   * @return
   */
  public RavenJObject patch(String key, ScriptedPatchRequest patch, boolean ignoreMissing) {
    ScriptedPatchCommandData command = new ScriptedPatchCommandData();
    command.setKey(key);
    command.setPatch(patch);

    BatchResult[] batchResults = batch(Arrays.<ICommandData> asList(command));
    if (!ignoreMissing && batchResults[0].getPatchResult() != null && batchResults[0].getPatchResult() == PatchResult.DOCUMENT_DOES_NOT_EXISTS) {
      throw new DocumentDoesNotExistsException("Document with key " + key + " does not exist.");
    }
    return batchResults[0].getAdditionalData();

  }

  /**
   * Sends a patch request for a specific document
   * @param key
   * @param patches
   * @param etag
   * @return
   */
  public RavenJObject patch(String key, PatchRequest[] patches, Etag etag) {
    PatchCommandData command = new PatchCommandData();
    command.setKey(key);
    command.setPatches(patches);
    command.setEtag(etag);

    BatchResult[] batchResults = batch(Arrays.<ICommandData> asList(command));
    return batchResults[0].getAdditionalData();
  }

  /**
   * Sends a patch request for a specific document which may or may not currently exist
   * @param key
   * @param patchesToExisting
   * @param patchesToDefault
   * @param defaultMetadata
   * @return
   */
  public RavenJObject patch(String key, PatchRequest[] patchesToExisting, PatchRequest[] patchesToDefault, RavenJObject defaultMetadata) {
    PatchCommandData command = new PatchCommandData();
    command.setKey(key);
    command.setPatches(patchesToExisting);
    command.setPatchesIfMissing(patchesToDefault);
    command.setMetadata(defaultMetadata);

    BatchResult[] batchResults = batch(Arrays.<ICommandData> asList(command));
    return batchResults[0].getAdditionalData();
  }

  /**
   * Sends a patch request for a specific document, ignoring the document's Etag
   * @param key
   * @param patch
   * @param etag
   * @return
   */
  public RavenJObject patch(String key, ScriptedPatchRequest patch, Etag etag) {
    ScriptedPatchCommandData command = new ScriptedPatchCommandData();
    command.setKey(key);
    command.setPatch(patch);
    command.setEtag(etag);

    BatchResult[] batchResults = batch(Arrays.<ICommandData> asList(command));
    return batchResults[0].getAdditionalData();
  }

  /**
   * Sends a patch request for a specific document which may or may not currently exist
   * @param key
   * @param patchExisting
   * @param patchDefault
   * @param defaultMetadata
   * @return
   */
  public RavenJObject patch(String key, ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata) {
    ScriptedPatchCommandData command = new ScriptedPatchCommandData();
    command.setKey(key);
    command.setPatch(patchExisting);
    command.setPatchIfMissing(patchDefault);
    command.setMetadata(defaultMetadata);

    BatchResult[] batchResults = batch(Arrays.<ICommandData>asList(command));
    return batchResults[0].getAdditionalData();
  }


  public AutoCloseable disableAllCaching() {
    return jsonRequestFactory.disableAllCaching();
  }

  /**
   * @return the profilingInformation
   */
  public ProfilingInformation getProfilingInformation() {
    return profilingInformation;
  }


  public RavenJToken getOperationStatus(long id) {
    HttpJsonRequest request = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, url + "/operation/status?id=" + id, HttpMethods.GET, new RavenJObject(), credentials, convention)
        .addOperationHeaders(operationsHeaders));
    try {
      return request.readResponseJson();
    } catch (HttpOperationException e) {
      try {
        if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
          return null;
        }
        throw e;
      } finally {
        EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
      }
    } catch (IOException e) {
      throw new ServerClientException(e);
    }
  }

  public boolean isExpect100Continue() {
    return expect100Continue;
  }

  public void setExpect100Continue(boolean expect100Continue) {
    this.expect100Continue = expect100Continue;
  }

}
