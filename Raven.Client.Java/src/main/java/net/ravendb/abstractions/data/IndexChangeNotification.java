package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.EventArgs;


public class IndexChangeNotification extends EventArgs {
  private IndexChangeTypes type;
  private String name;
  private Etag etag;

  @Override
  public String toString() {
    return String.format("%s on %s", type, name);
  }

  public IndexChangeTypes getType() {
    return type;
  }

  public void setType(IndexChangeTypes type) {
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Etag getEtag() {
    return etag;
  }

  public void setEtag(Etag etag) {
    this.etag = etag;
  }
}
