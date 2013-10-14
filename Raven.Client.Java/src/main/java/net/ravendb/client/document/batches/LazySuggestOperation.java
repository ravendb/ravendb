package net.ravendb.client.document.batches;

import java.util.List;

import net.ravendb.abstractions.basic.SharpEnum;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.data.GetRequest;
import net.ravendb.abstractions.data.GetResponse;
import net.ravendb.abstractions.data.SuggestionQuery;
import net.ravendb.abstractions.data.SuggestionQueryResult;
import net.ravendb.abstractions.json.linq.RavenJObject;

import org.apache.http.HttpStatus;


public class LazySuggestOperation implements ILazyOperation {

  private final String index;
  private final SuggestionQuery suggestionQuery;
  private Object result;
  private boolean requiresRetry;

  @Override
  public Object getResult() {
    return result;
  }

  @Override
  public boolean isRequiresRetry() {
    return requiresRetry;
  }

  public LazySuggestOperation(String index, SuggestionQuery suggestionQuery) {
    this.index = index;
    this.suggestionQuery = suggestionQuery;
  }

  @Override
  public GetRequest createRequest() {
    String query = String.format("term=%s&field=%s&max=%d", suggestionQuery.getTerm(), suggestionQuery.getField(), suggestionQuery.getMaxSuggestions());
    if (suggestionQuery.getAccuracy() != null) {
      query += "&accuracy=" + String.format(Constants.getDefaultLocale(), "%.3f", suggestionQuery.getAccuracy());
    }
    if (suggestionQuery.getDistance() != null) {
      query += "&distance=" + SharpEnum.value(suggestionQuery.getDistance());
    }

    GetRequest  getRequest = new GetRequest();
    getRequest.setUrl("/suggest/" + index);
    getRequest.setQuery(query);
    return getRequest;
  }

  @Override
  public void handleResponse(GetResponse response) {
    if (response.getStatus() != HttpStatus.SC_OK && response.getStatus() != HttpStatus.SC_NOT_MODIFIED) {
      throw new IllegalStateException("Got an unexpected response code for the request: " + response.getStatus() + "\n" + response.getResult());
    }

    RavenJObject result = (RavenJObject) response.getResult();
    SuggestionQueryResult suggestionQueryResult = new SuggestionQueryResult();
    List<String> values = result.get("Suggestions").values(String.class);

    suggestionQueryResult.setSuggestions(values.toArray(new String[0]));
    this.result = suggestionQueryResult;
  }

  @Override
  public AutoCloseable enterContext() {
    return null;
  }
}
