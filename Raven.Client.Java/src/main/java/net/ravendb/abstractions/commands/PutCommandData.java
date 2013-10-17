package net.ravendb.abstractions.commands;

import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJValue;

/**
 *  A single batch operation for a document PUT
 */
public class PutCommandData implements ICommandData {
  private String key;
  private Etag etag;
  private RavenJObject document;
  private RavenJObject metadata;
  private RavenJObject additionalData;


  public PutCommandData() {
    super();
  }

  public PutCommandData(String key, Etag etag, RavenJObject document, RavenJObject metadata) {
    super();
    this.key = key;
    this.etag = etag;
    this.document = document;
    this.metadata = metadata;
  }

  @Override
  public RavenJObject getAdditionalData() {
    return additionalData;
  }

  public RavenJObject getDocument() {
    return document;
  }

  @Override
  public Etag getEtag() {
    return etag;
  }

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public RavenJObject getMetadata() {
    return metadata;
  }

  @Override
  public HttpMethods getMethod() {
    return HttpMethods.PUT;
  }

  @Override
  public void setAdditionalData(RavenJObject additionalData) {
    this.additionalData = additionalData;
  }

  public void setDocument(RavenJObject document) {
    this.document = document;
  }

  public void setEtag(Etag etag) {
    this.etag = etag;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setMetadata(RavenJObject metadata) {
    this.metadata = metadata;
  }

  @Override
  public RavenJObject toJson() {
    RavenJObject value = new RavenJObject();
    value.add("Key", new RavenJValue(key));
    value.add("Method", new RavenJValue(getMethod().name()));
    value.add("Document", document);
    value.add("Metadata", metadata);
    value.add("AdditionalData", additionalData);

    if (etag != null) {
      value.add("Etag", new RavenJValue(etag.toString()));
    }

    return value;
  }

}
