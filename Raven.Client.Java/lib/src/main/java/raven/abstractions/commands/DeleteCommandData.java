package raven.abstractions.commands;

import raven.abstractions.data.Etag;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.data.TransactionInformation;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJValue;

/**
 * A single batch operation for a document DELETE
 */
public class DeleteCommandData implements ICommandData {
  private String key;
  private Etag etag;
  private TransactionInformation transactionInformation;
  private RavenJObject additionalData;

  public DeleteCommandData(String key, Etag etag) {
    super();
    this.key = key;
    this.etag = etag;
  }

  public DeleteCommandData() {
    super();
  }

  public RavenJObject getAdditionalData() {
    return additionalData;
  }

  public Etag getEtag() {
    return etag;
  }

  public String getKey() {
    return key;
  }

  public RavenJObject getMetadata() {
    return null;
  }

  public HttpMethods getMethod() {
    return HttpMethods.DELETE;
  }

  public TransactionInformation getTransactionInformation() {
    return transactionInformation;
  }

  public void setAdditionalData(RavenJObject additionalData) {
    this.additionalData = additionalData;
  }

  public void setEtag(Etag etag) {
    this.etag = etag;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setTransactionInformation(TransactionInformation transactionInformation) {
    this.transactionInformation = transactionInformation;
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
