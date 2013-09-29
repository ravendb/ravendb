package raven.client.document.batches;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
  private final String transformer;
  private Object result;
  private boolean requiresRetry;
  private Class<T> clazz;
  public LazyLoadOperation(Class<T> clazz, String key, LoadOperation loadOperation) {
    this (clazz, key, loadOperation, null);
  }

  public LazyLoadOperation(Class<T> clazz, String key, LoadOperation loadOperation, String transformer) {
    this.key = key;
    this.loadOperation = loadOperation;
    this.clazz = clazz;
    this.transformer = transformer;
  }

  @Override
  public GetRequest createRequest() {
    String path = null;
    if (StringUtils.isNotEmpty(transformer)) {
      path = "/queries/"     + UrlUtils.escapeDataString(key);
      if (StringUtils.isNotEmpty(transformer)) {
        path += "&transformer=" + transformer;
      }
    } else {
      path = "/docs/" + UrlUtils.escapeDataString(key);
    }

    GetRequest request = new GetRequest();
    request.setUrl(path);
    return request;
  }

  @Override
  public Object getResult() {
    return result;
  }

  public void setResult(Object result) {
    this.result = result;
  }

  @Override
  public boolean isRequiresRetry() {
    return requiresRetry;
  }

  public void setRequiresRetry(boolean requiresRetry) {
    this.requiresRetry = requiresRetry;
  }

  @Override
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

  @Override
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
