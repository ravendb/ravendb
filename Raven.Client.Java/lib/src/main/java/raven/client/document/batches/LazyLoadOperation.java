package raven.client.document.batches;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpStatus;

import raven.abstractions.data.GetRequest;
import raven.abstractions.data.GetResponse;
import raven.abstractions.data.JsonDocument;
import raven.client.connection.IDatabaseCommands;
import raven.client.connection.SerializationHelper;
import raven.client.document.sessionoperations.LoadOperation;
import raven.client.utils.UrlUtils;

public class LazyLoadOperation<T> implements ILazyOperation {

  private final String key;
  private final LoadOperation loadOperation;
  private Object result;
  private boolean requiresRetry;
  private Class<T> clazz;

  public LazyLoadOperation(Class<T> clazz, String key, LoadOperation loadOperation) {
    this.key = key;
    this.loadOperation = loadOperation;
    this.clazz = clazz;
  }

  public GetRequest createRequest() {
    GetRequest request = new GetRequest();
    request.setUrl("/docs/" + UrlUtils.escapeDataString(key));
    return request;
  }

  public Object getResult() {
    return result;
  }

  public void setResult(Object result) {
    this.result = result;
  }

  public boolean isRequiresRetry() {
    return requiresRetry;
  }

  public void setRequiresRetry(boolean requiresRetry) {
    this.requiresRetry = requiresRetry;
  }

  public void handleResponse(GetResponse response) {
    if(response.getStatus() == HttpStatus.SC_NOT_FOUND) {
      result = null;
      requiresRetry = false;
      return;
    }

    Map<String, String> headers = new HashMap<>(response.getHeaders());
    JsonDocument jsonDocument = SerializationHelper.deserializeJsonDocument(key, response.getResult(), headers, response.getStatus());
    handleResponse(jsonDocument);
  }

  private void handleResponse(JsonDocument jsonDocument) {
    requiresRetry = loadOperation.setResult(jsonDocument);
    if (requiresRetry == false) {
      result = loadOperation.complete(clazz);
    }
  }

  public AutoCloseable enterContext() {
    return loadOperation.enterLoadContext();
  }

  public Object executeEmbedded(IDatabaseCommands commands) {
    return commands.get(key);
  }

  public void handleEmbeddedResponse(Object result) {
    handleResponse((JsonDocument) result);
  }

}
