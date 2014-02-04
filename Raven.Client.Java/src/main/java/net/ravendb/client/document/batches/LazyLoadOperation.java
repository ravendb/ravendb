package net.ravendb.client.document.batches;

import java.util.HashMap;
import java.util.Map;

import net.ravendb.abstractions.data.GetRequest;
import net.ravendb.abstractions.data.GetResponse;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.data.QueryResult;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.client.connection.SerializationHelper;
import net.ravendb.client.document.sessionoperations.LoadOperation;
import net.ravendb.client.utils.UrlUtils;

import org.apache.http.HttpStatus;


public class LazyLoadOperation<T> implements ILazyOperation {

  private final String key;
  private final LoadOperation loadOperation;
  private Object result;
  private boolean requiresRetry;
  private QueryResult queryResult;
  private Class<T> clazz;

  public LazyLoadOperation(Class<T> clazz, String key, LoadOperation loadOperation) {
    this.key = key;
    this.loadOperation = loadOperation;
    this.clazz = clazz;
  }

  @Override
  public GetRequest createRequest() {
    String path = "/docs";
    String query = "id=" + UrlUtils.escapeDataString(key);

    GetRequest request = new GetRequest();
    request.setUrl(path);
    request.setQuery(query);
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

  @Override
  public QueryResult getQueryResult() {
    return queryResult;
  }

  public void setQueryResult(QueryResult queryResult) {
    this.queryResult = queryResult;
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
