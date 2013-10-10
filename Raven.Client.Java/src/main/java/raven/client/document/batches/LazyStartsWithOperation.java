package raven.client.document.batches;

import java.util.ArrayList;
import java.util.List;

import raven.abstractions.data.GetRequest;
import raven.abstractions.data.GetResponse;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.json.linq.RavenJArray;
import raven.abstractions.json.linq.RavenJObject;
import raven.client.connection.IDatabaseCommands;
import raven.client.connection.SerializationHelper;
import raven.client.document.InMemoryDocumentSessionOperations;
import raven.client.utils.UrlUtils;

public class LazyStartsWithOperation<T> implements ILazyOperation {
  private final String keyPrefix;
  private final String matches;
  private final String exclude;
  private final int start;
  private final int pageSize;
  private final InMemoryDocumentSessionOperations sessionOperations;
  private final Class<T> clazz;

  private Object result;
  private boolean requiresRetry;

  public LazyStartsWithOperation(Class<T> clazz, String keyPrefix, String matches, String exclude, int start, int pageSize, InMemoryDocumentSessionOperations sessionOperations) {
    this.clazz = clazz;
    this.keyPrefix = keyPrefix;
    this.matches = matches;
    this.exclude = exclude;
    this.start = start;
    this.pageSize = pageSize;
    this.sessionOperations = sessionOperations;
  }

  @Override
  public GetRequest createRequest() {
    GetRequest getRequest = new GetRequest();
    getRequest.setUrl("/docs");
    getRequest.setQuery(String.format("startsWith=%s&matches=%s&exclude=%s&start=%d&pageSize=%d", UrlUtils.escapeDataString(keyPrefix),
        UrlUtils.escapeDataString(matches != null ? matches : ""),
        UrlUtils.escapeDataString(exclude != null ? exclude : ""),
        start, pageSize));
    return getRequest;
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
    if (response.isRequestHasErrors()) {
      result = null;
      requiresRetry = false;
      return;
    }

    List<JsonDocument> jsonDocuments = SerializationHelper.ravenJObjectsToJsonDocuments(((RavenJArray)response.getResult()).values(RavenJObject.class));

    List<Object> resultList = new ArrayList<>();
    for (JsonDocument doc: jsonDocuments) {
      resultList.add(sessionOperations.trackEntity(clazz, doc));
    }
    result = resultList.toArray();
  }

  @Override
  public AutoCloseable enterContext() {
    return null;
  }

  public Object executeEmbedded(IDatabaseCommands commands) {
    List<JsonDocument> docs = commands.startsWith(keyPrefix, matches, start, pageSize);
    List<Object> entites = new ArrayList<>();
    for (JsonDocument doc: docs) {
      entites.add(sessionOperations.trackEntity(clazz, doc));
    }
    return entites.toArray();
  }

  public void handleEmbeddedResponse(Object result) {
    this.result = result;
  }


}
