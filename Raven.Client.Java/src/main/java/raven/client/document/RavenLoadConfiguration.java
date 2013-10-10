package raven.client.document;

import java.util.HashMap;
import java.util.Map;

import raven.abstractions.json.linq.RavenJToken;
import raven.client.ILoadConfiguration;

public class RavenLoadConfiguration implements ILoadConfiguration {
  private Map<String, RavenJToken> queryInputs = new HashMap<>();

  public Map<String, RavenJToken> getQueryInputs() {
    return queryInputs;
  }

  public void setQueryInputs(Map<String, RavenJToken> queryInputs) {
    this.queryInputs = queryInputs;
  }

  @Override
  public void addQueryParam(String name, RavenJToken value) {
    queryInputs.put(name, value);
  }



}
