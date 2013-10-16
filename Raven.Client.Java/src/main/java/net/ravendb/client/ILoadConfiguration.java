package net.ravendb.client;

import net.ravendb.abstractions.json.linq.RavenJToken;

public interface ILoadConfiguration {
  void addQueryParam(String name, RavenJToken value);
}
