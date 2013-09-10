package raven.client.document.batches;

import java.util.List;

import org.apache.http.HttpStatus;

import raven.abstractions.basic.SharpEnum;
import raven.abstractions.data.Constants;
import raven.abstractions.data.GetRequest;
import raven.abstractions.data.GetResponse;
import raven.abstractions.data.SuggestionQuery;
import raven.abstractions.data.SuggestionQueryResult;
import raven.abstractions.json.linq.RavenJObject;

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
