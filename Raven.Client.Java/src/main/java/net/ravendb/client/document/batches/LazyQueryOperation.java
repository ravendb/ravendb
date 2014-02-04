package net.ravendb.client.document.batches;

import java.util.Map;
import java.util.Set;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.GetRequest;
import net.ravendb.abstractions.data.GetResponse;
import net.ravendb.abstractions.data.QueryResult;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.connection.HttpExtensions;
import net.ravendb.client.connection.SerializationHelper;
import net.ravendb.client.document.sessionoperations.QueryOperation;

import org.apache.http.HttpStatus;


public class LazyQueryOperation<T> implements ILazyOperation {
  private final QueryOperation queryOperation;
  private final Action1<QueryResult> afterQueryExecuted;
  private final Set<String> includes;
  private final Class<T> clazz;

  private QueryResult queryResult;

  private Map<String, String> headers;

  private Object result;
  private boolean requiresRetry;

  public LazyQueryOperation(Class<T> clazz, QueryOperation queryOperation, Action1<QueryResult> afterQueryExecuted, Set<String> includes) {
    this.clazz = clazz;
    this.queryOperation = queryOperation;
    this.afterQueryExecuted = afterQueryExecuted;
    this.includes = includes;
  }

  @Override
  public QueryResult getQueryResult() {
    return queryResult;
  }

  public void setQueryResult(QueryResult queryResult) {
    this.queryResult = queryResult;
  }



  @Override
  public GetRequest createRequest() {
    StringBuilder stringBuilder = new StringBuilder();
    queryOperation.getIndexQuery().appendQueryString(stringBuilder);

    for (String include : includes) {
      stringBuilder.append("&include=").append(include);
    }
    GetRequest request = new GetRequest();
    request.setUrl("/indexes/" + queryOperation.getIndexName());
    request.setQuery(stringBuilder.toString());

    if (headers != null) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        request.getHeaders().put(header.getKey(), header.getValue());
      }
    }
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
    if (response.getStatus() == HttpStatus.SC_NOT_FOUND) {
      throw new IllegalStateException("There is no index named: " + queryOperation.getIndexName() + "\n" + response.getResult());
    }
    RavenJObject json = (RavenJObject)response.getResult();
    QueryResult queryResult = SerializationHelper.toQueryResult(json, HttpExtensions.getEtagHeader(response), response.getHeaders().get("Temp-Request-Time"));
    handleResponse(queryResult);
  }

  private void handleResponse(QueryResult queryResult) {
    requiresRetry = queryOperation.isAcceptable(queryResult) == false;
    if (requiresRetry) {
      return;
    }

    if (afterQueryExecuted != null)
      afterQueryExecuted.apply(queryResult);
    result = queryOperation.complete(clazz);
    this.queryResult = queryResult;
  }

  @Override
  public AutoCloseable enterContext() {
    return queryOperation.enterQueryContext();
  }

  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }


}
