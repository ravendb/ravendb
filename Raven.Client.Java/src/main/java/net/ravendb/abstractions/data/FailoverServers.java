package net.ravendb.abstractions.data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class FailoverServers {
  private final Set<String> forDefaultDatabase = new HashSet<>();
  private final Map<String, Set<String>> forDatabases = new HashMap<>();

  public String[] getForDefaultDatabase() {
    return forDefaultDatabase.toArray(new String[0]);
  }

  public void setForDefaultDatabase(Set<String> value) {
    addForDefaultDatabase(value.toArray(new String[0]));
  }

  public void setForDatabases(Map<String, String[]> value) {
    for (Map.Entry<String, String[]> entry : value.entrySet()) {
      addForDatabase(entry.getKey(), entry.getValue());
    }
  }

  public boolean isSetForDefaultDatabase() {
    return forDefaultDatabase.size() > 0;
  }

  public boolean isSetForDatabase(String databaseName) {
    return forDatabases.containsKey(databaseName) && forDatabases.get(databaseName) != null && forDatabases.get(databaseName).size() > 0;
  }

  public String[] getForDatabase(String databaseName) {
    if (!forDatabases.containsKey(databaseName) || forDatabases.get(databaseName) == null) {
      return null;
    }
    return forDatabases.get(databaseName).toArray(new String[0]);
  }

  public void addForDefaultDatabase(String... urls) {
    for (String url : urls) {
      forDefaultDatabase.add(url.endsWith("/") ? url.substring(0, url.length() - 1) : url);
    }
  }

  public void addForDatabase(String databaseName, String... urls) {
    if (!forDatabases.containsKey(databaseName)) {
      forDatabases.put(databaseName, new HashSet<String>());
    }
    for (String url : urls) {
      forDatabases.get(databaseName).add(url.endsWith("/") ? url.substring(0, url.length() - 1) : url);
    }
  }

}
