package raven.client.connection;

import java.util.UUID;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.HttpsURL;

import raven.client.data.Constants;
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
        new CreateHttpJsonRequestParams(this, serverUrl + "/docs/" + UrlUtils.escapeDataString(key), "DELETE"))) {
      jsonRequest.executeDeleteRequest();
    } catch (Exception e) {
      throw new ServerClientException(e);
    }

  }

  @Override
  public JsonDocument get(String key) throws ServerClientException {
    //TODO: support for replication
    return directGet(url, key);
  }

  /**
   * Perform a direct get for a document with the specified key on the specified server URL.
   * @param serverUrl
   * @param key
   * @return
   */
  private JsonDocument directGet(String serverUrl, String key) throws ServerClientException {
    try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(
        new CreateHttpJsonRequestParams(this, serverUrl + "/docs/" + UrlUtils.escapeDataString(key), "GET"))) {
      RavenJToken responseJson = jsonRequest.getResponseAsJson(HttpStatus.SC_OK);

      String docKey = jsonRequest.getResponseHeader(Constants.DOCUMENT_ID_FIELD_NAME);
      if (docKey == null) {
        docKey = key;
      }
      docKey = UrlUtils.unescapeDataString(docKey);

      return SerializationHelper.deserializeJsonDocument(docKey, responseJson, jsonRequest);

    } catch (Exception e) {
      if (e instanceof HttpOperationException) {
        HttpOperationException httpException = (HttpOperationException) e;
        if (httpException.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
          return null;
        }
        //TODO: resolve conflicts
      } else {
        throw new ServerClientException(e);
      }
    }
    return null;
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
    String method = StringUtils.isNotNullOrEmpty(key) ? "PUT" : "POST";
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


}
