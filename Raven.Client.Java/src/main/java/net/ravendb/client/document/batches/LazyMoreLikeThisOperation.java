package net.ravendb.client.document.batches;

import net.ravendb.abstractions.data.GetRequest;
import net.ravendb.abstractions.data.GetResponse;
import net.ravendb.abstractions.data.MoreLikeThisQuery;
import net.ravendb.abstractions.data.MultiLoadResult;
import net.ravendb.abstractions.data.QueryResult;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.client.document.sessionoperations.MultiLoadOperation;


public class LazyMoreLikeThisOperation<T> implements ILazyOperation {

  private final MultiLoadOperation multiLoadOperation;
  private final MoreLikeThisQuery query;
  private Class<T> clazz;
  private QueryResult queryResult;
  private Object result;
  private boolean requiresRetry;


  @Override
  public QueryResult getQueryResult() {
    return queryResult;
  }


  public void setQueryResult(QueryResult queryResult) {
    this.queryResult = queryResult;
  }

  @Override
  public Object getResult() {
    return result;
  }

  @Override
  public boolean isRequiresRetry() {
    return requiresRetry;
  }


  public LazyMoreLikeThisOperation(Class<T> clazz, MultiLoadOperation multiLoadOperation, MoreLikeThisQuery query) {
    super();
    this.multiLoadOperation = multiLoadOperation;
    this.query = query;
  }


  @Override
  public GetRequest createRequest() {
    String uri = query.getRequestUri();
    int separator = uri.indexOf('?');
    return new GetRequest(uri.substring(0, separator), uri.substring(separator + 1, uri.length() - separator - 1));
  }

  @Override
  public void handleResponse(GetResponse response) {
    RavenJToken result = response.getResult();
    MultiLoadResult multiLoadResult = new MultiLoadResult();

    multiLoadResult.setIncludes(result.value(RavenJArray.class, "Includes").values(RavenJObject.class));
    multiLoadResult.setResults(result.value(RavenJArray.class, "Results").values(RavenJObject.class));

    handleResponse(multiLoadResult);
  }

  private void handleResponse(MultiLoadResult multiLoadResult) {
    requiresRetry = multiLoadOperation.setResult(multiLoadResult);
    if (requiresRetry == false) {
      result = multiLoadOperation.complete(clazz);
    }
  }

  @Override
  public AutoCloseable enterContext() {
    return multiLoadOperation.enterMultiLoadContext();
  }

}
