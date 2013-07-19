package raven.abstractions.commands;

import raven.abstractions.data.Etag;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.data.TransactionInformation;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJValue;

/**
 *  A single batch operation for a document PUT
 */
public class PutCommandData implements ICommandData {
  private String key;
  private Etag etag;
  private RavenJObject document;
  private TransactionInformation transactionInformation;
  private RavenJObject metadata;
  private RavenJObject additionalData;

  public RavenJObject getAdditionalData() {
    return additionalData;
  }

  public RavenJObject getDocument() {
    return document;
  }

  public Etag getEtag() {
    return etag;
  }

  public String getKey() {
    return key;
  }

  public RavenJObject getMetadata() {
    return metadata;
  }

  public HttpMethods getMethod() {
    return HttpMethods.PUT;
  }

  public TransactionInformation getTransactionInformation() {
    return transactionInformation;
  }

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

  public void setTransactionInformation(TransactionInformation transactionInformation) {
    this.transactionInformation = transactionInformation;
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
