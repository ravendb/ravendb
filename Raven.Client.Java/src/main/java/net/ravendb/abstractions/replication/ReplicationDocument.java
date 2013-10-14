package net.ravendb.abstractions.replication;

import java.util.ArrayList;
import java.util.List;


public class ReplicationDocument {

  private List<ReplicationDestination> destinations;
  private String id;
  private String source;

  public ReplicationDocument() {
    id = "Raven/Replication/Destinations";
    destinations = new ArrayList<>();
  }

  public List<ReplicationDestination> getDestinations() {
    return destinations;
  }

  public String getId() {
    return id;
  }

  public String getSource() {
    return source;
  }

  public void setDestinations(List<ReplicationDestination> destinations) {
    this.destinations = destinations;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setSource(String source) {
    this.source = source;
  }


}
