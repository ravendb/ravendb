package net.ravendb.abstractions.data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.ravendb.abstractions.replication.ReplicationDestination;


public class FailoverServers {
  private final Set<ReplicationDestination> forDefaultDatabase = new HashSet<>();
  private final Map<String, Set<ReplicationDestination>> forDatabases = new HashMap<>();

  public ReplicationDestination[] getForDefaultDatabase() {
    return forDefaultDatabase.toArray(new ReplicationDestination[0]);
  }

  public void setForDefaultDatabase(Set<String> value) {
    addForDefaultDatabase(value.toArray(new ReplicationDestination[0]));
  }

  public void setForDatabases(Map<String, ReplicationDestination[]> value) {
    for (Map.Entry<String, ReplicationDestination[]> entry : value.entrySet()) {
      addForDatabase(entry.getKey(), entry.getValue());
    }
  }

  public boolean isSetForDefaultDatabase() {
    return forDefaultDatabase.size() > 0;
  }

  public boolean isSetForDatabase(String databaseName) {
    return forDatabases.containsKey(databaseName) && forDatabases.get(databaseName) != null && forDatabases.get(databaseName).size() > 0;
  }

  public ReplicationDestination[] getForDatabase(String databaseName) {
    if (!forDatabases.containsKey(databaseName) || forDatabases.get(databaseName) == null) {
      return null;
    }
    return forDatabases.get(databaseName).toArray(new ReplicationDestination[0]);
  }

  public void addForDefaultDatabase(ReplicationDestination... destinations) {
    for (ReplicationDestination dest : destinations) {
      forDefaultDatabase.add(dest);
    }
  }

  public void addForDatabase(String databaseName, ReplicationDestination... destinations) {
    if (!forDatabases.containsKey(databaseName)) {
      forDatabases.put(databaseName, new HashSet<ReplicationDestination>());
    }
    for (ReplicationDestination dest : destinations) {
      forDatabases.get(databaseName).add(dest);
    }
  }

}
