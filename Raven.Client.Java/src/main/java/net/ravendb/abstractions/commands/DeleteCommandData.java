package net.ravendb.abstractions.commands;

import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJValue;

/**
 * A single batch operation for a document DELETE
 */
public class DeleteCommandData implements ICommandData {
  private String key;
  private Etag etag;
  private RavenJObject additionalData;

  public DeleteCommandData(String key, Etag etag) {
    super();
    this.key = key;
    this.etag = etag;
  }

  public DeleteCommandData() {
    super();
  }

  @Override
  public RavenJObject getAdditionalData() {
    return additionalData;
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
    return null;
  }

  @Override
  public HttpMethods getMethod() {
    return HttpMethods.DELETE;
  }

  @Override
  public void setAdditionalData(RavenJObject additionalData) {
    this.additionalData = additionalData;
  }

  public void setEtag(Etag etag) {
    this.etag = etag;
  }

  public void setKey(String key) {
    this.key = key;
  }

  @Override
  public RavenJObject toJson() {
    RavenJObject object = new RavenJObject();
    object.add("Key", new RavenJValue(key));
    object.add("Etag", new RavenJValue(etag != null ? etag.toString() : null));
    object.add("Method", new RavenJValue(getMethod().name()));
    object.add("AdditionalData", additionalData);
    return object;
  }

}
