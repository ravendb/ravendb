package net.ravendb.abstractions.data;

import net.ravendb.abstractions.json.linq.RavenJObject;


public class AttachmentInformation {
  private int size;
  private String key;
  private RavenJObject metadata;
  private Etag etag;

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public RavenJObject getMetadata() {
    return metadata;
  }

  public void setMetadata(RavenJObject metadata) {
    this.metadata = metadata;
  }

  public Etag getEtag() {
    return etag;
  }

  public void setEtag(Etag etag) {
    this.etag = etag;
  }

}
