package net.ravendb.abstractions.data;

import net.ravendb.abstractions.json.linq.RavenJObject;

public class StreamResult<T> {
  private String key;
  private Etag etag;
  private RavenJObject metadata;
  private T document;

  public String getKey() {
    return key;
  }
  public void setKey(String key) {
    this.key = key;
  }
  public Etag getEtag() {
    return etag;
  }
  public void setEtag(Etag etag) {
    this.etag = etag;
  }
  public RavenJObject getMetadata() {
    return metadata;
  }
  public void setMetadata(RavenJObject metadata) {
    this.metadata = metadata;
  }
  public T getDocument() {
    return document;
  }
  public void setDocument(T document) {
    this.document = document;
  }


}
