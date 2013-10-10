package raven.client;

import raven.abstractions.json.linq.RavenJToken;

public interface ILoadConfiguration {
  void addQueryParam(String name, RavenJToken value);
}
