package net.ravendb.client.document.batches;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import net.ravendb.abstractions.data.GetRequest;
import net.ravendb.abstractions.data.GetResponse;
import net.ravendb.abstractions.data.MultiLoadResult;
import net.ravendb.abstractions.data.QueryResult;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.document.sessionoperations.LoadTransformerOperation;
import net.ravendb.client.utils.UrlUtils;


public class LazyTransformerLoadOperation<T> implements ILazyOperation {

  private Class<T> clazz;
  private String[] ids;
  private String transformer;
  private LoadTransformerOperation loadTransformerOperation;
  private boolean singleResult;
  private Object result;
  private boolean requiresRetry;
  private QueryResult queryResult;


  @Override
  public QueryResult getQueryResult() {
    return queryResult;
  }


  public void setQueryResult(QueryResult queryResult) {
    this.queryResult = queryResult;
  }

  public LazyTransformerLoadOperation(Class<T> clazz, String[] ids, String transformer, LoadTransformerOperation loadTransformerOperation, boolean singleResult) {
    this.clazz = clazz;
    this.ids = ids;
    this.transformer = transformer;
    this.loadTransformerOperation = loadTransformerOperation;
    this.singleResult = singleResult;
  }

  @Override
  public GetRequest createRequest() {
    List<String> tokens = new ArrayList<>();
    for (String id: ids) {
      tokens.add("id=" + UrlUtils.escapeDataString(id));
    }
    String query = "?" + StringUtils.join(tokens, "&");
    if (StringUtils.isNotEmpty(transformer)) {
      query += "&transformer=" + transformer;
    }

    return new GetRequest("/queries/", query);
  }

  @Override
  public Object getResult() {
    return result;
  }

  @Override
  public boolean isRequiresRetry() {
    return requiresRetry;
  }

  @Override
  public void handleResponse(GetResponse response) {
    if (response.isRequestHasErrors()) {
      throw new IllegalStateException("Got bad status code: " + response.getStatus());
    }
    MultiLoadResult multiLoadResult = new MultiLoadResult();
    multiLoadResult.setIncludes(response.getResult().value(RavenJArray.class, "Includes").values(RavenJObject.class));
    multiLoadResult.setResults(response.getResult().value(RavenJArray.class, "Results").values(RavenJObject.class));
    handleResponse(multiLoadResult);
  }

  @Override
  public AutoCloseable enterContext() {
    return null;
  }

  private void handleResponse(MultiLoadResult multiLoadResult) {
    T[] complete = loadTransformerOperation.complete(clazz, multiLoadResult);
    result = singleResult ? complete[0] : complete;

  }
}
