package raven.client.connection;

import java.util.List;
import java.util.UUID;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.HttpsURL;

import raven.client.data.Constants;
import raven.client.extensions.MultiDatabase;
import raven.client.json.JsonDocument;
import raven.client.json.PutResult;
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
  private HttpJsonRequestFactory jsonRequestFactory;

  /**
   * @return the httpClient
   */
  protected HttpClient getHttpClient() {
    return httpClient;
  }

  public ServerClient(String url) {
    super();
    if (url.endsWith("/")) {
      url = url.substring(0, url.length() - 1);
    }

    this.url = url;
    httpClient = new HttpClient();
    jsonRequestFactory = new HttpJsonRequestFactory();
  }

  @Override
  public void delete(String key, UUID etag) throws ServerClientException {
    //TODO: support for replication
    directDelete(url, key, etag);
  }

  private void directDelete(String serverUrl, String key, UUID etag) throws ServerClientException{
    RavenJObject metadata = new RavenJObject();
    if (etag != null) {
      metadata.add("ETag", RavenJToken.fromObject(etag.toString()));
    }
    try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, serverUrl + "/docs/" + UrlUtils.escapeDataString(key), HttpMethods.DELETE))) {
      jsonRequest.executeDeleteRequest();
    } catch (Exception e) {
      throw new ServerClientException(e);
    }

  }

  @Override
  public JsonDocument get(String key) throws ServerClientException {
    ensureIsNotNullOrEmpty(key, "key");
    //TODO: support for replication
    return directGet(url, key);
  }

  private void ensureIsNotNullOrEmpty(String key, String argName) {
    if (key == null || "".equals(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty " + argName);
    }

  }

  /**
   * Perform a direct get for a document with the specified key on the specified server URL.
   * @param serverUrl
   * @param key
   * @return
   */
  private JsonDocument directGet(String serverUrl, String key) throws ServerClientException {
    try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, serverUrl + "/docs/" + UrlUtils.escapeDataString(key), HttpMethods.GET))) {
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
      }
      //TODO: resolve conflicts
    } catch (Exception e) {
        throw new ServerClientException(e);
    }
    return null;
  }

  private List<JsonDocument> directStartsWith(String serverUrl, String keyPrefix, String matches, int start, int pageSize, boolean metadataOnly) throws ServerClientException {
    String actualUrl = serverUrl + String.format("/docs?startsWith=%s&matches=%s&start=%d&pageSize=%d", UrlUtils.escapeDataString(keyPrefix),
        StringUtils.defaultIfNull(matches, ""), start, pageSize);
    if (metadataOnly) {
      actualUrl += "&metadata-only=true";
    }
    try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, actualUrl, HttpMethods.GET))) {

      RavenJToken responseJson = jsonRequest.getResponseAsJson(HttpStatus.SC_OK);
      return SerializationHelper.ravenJObjectsToJsonDocuments(responseJson);
    } catch (Exception e) {
      //TODO: resolve conflicts
      throw new ServerClientException(e);
    }
  }

  /**
   * @return the url
   */
  public String getUrl() {
    return url;
  }

  @Override
  public PutResult put(String key, UUID etag, RavenJObject document, RavenJObject metadata) throws ServerClientException {
    return directPut(metadata, key,  etag, document, url);
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

    try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUrl, method))) {
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

  private ConcurrencyException throwConcurrencyException(Exception e) {
    UUID expectedEtag  = null;
    UUID actualEtag = null;
    //TODO: implement me!
    return new ConcurrencyException(expectedEtag, actualEtag, e);
  }

  public IDatabaseCommands forSystemDatabase() {
    String databaseUrl = MultiDatabase.getRootDatabaseUrl(url);
    if (databaseUrl.equals(url)) {
      return this;
    }
    return new ServerClient(databaseUrl);
  }

  public IDatabaseCommands forDatabase(String database) {
    String databaseUrl = MultiDatabase.getRootDatabaseUrl(url);
    databaseUrl = url + "/databases/" + database;
    if (databaseUrl.equals(url)) {
      return this;
    }
    return new ServerClient(databaseUrl);
  }

  @Override
  public List<JsonDocument> startsWith(String keyPrefix, String matches, int start, int pageSize, boolean metadataOnly) throws ServerClientException {
    ensureIsNotNullOrEmpty(keyPrefix, "keyPrefix");
    //TODO: replication
    return directStartsWith(url, keyPrefix, matches, start, pageSize, metadataOnly);
  }




}
