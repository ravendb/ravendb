package raven.abstractions.commands;

import raven.abstractions.basic.SharpEnum;
import raven.abstractions.data.Etag;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.data.PatchRequest;
import raven.abstractions.data.TransactionInformation;
import raven.abstractions.json.linq.RavenJArray;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJValue;

public class PatchCommandData implements ICommandData {

  private PatchRequest[] patches;
  private PatchRequest[] patchesIfMissing;
  private String key;
  private Etag etag;
  private TransactionInformation transactionInformation;
  private RavenJObject metadata;
  private RavenJObject additionalData;

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
    return metadata;
  }

  public HttpMethods getMethod() {
    return HttpMethods.PATCH;
  }

  public PatchRequest[] getPatches() {
    return patches;
  }

  public PatchRequest[] getPatchesIfMissing() {
    return patchesIfMissing;
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

  public void setMetadata(RavenJObject metadata) {
    this.metadata = metadata;
  }

  public void setPatches(PatchRequest[] patches) {
    this.patches = patches;
  }

  public void setPatchesIfMissing(PatchRequest[] patchesIfMissing) {
    this.patchesIfMissing = patchesIfMissing;
  }

  public void setTransactionInformation(TransactionInformation transactionInformation) {
    this.transactionInformation = transactionInformation;
  }

  @Override
  public RavenJObject toJson() {
    RavenJObject ret = new RavenJObject();
    ret.add("Key", new RavenJValue(key));
    ret.add("Method", new RavenJValue(SharpEnum.value(getMethod())));

    RavenJArray patchesArray = new RavenJArray();
    for (PatchRequest patchRequest: patches) {
      patchesArray.add(patchRequest.toJson());
    }
    ret.add("Patches", patchesArray);
    ret.add("AdditionalData", additionalData);
    if (etag != null) {
      ret.add("Etag", new RavenJValue(etag));
    }
    if (patchesIfMissing != null && patchesIfMissing.length > 0) {
      RavenJArray patchesIfMissingArray = new RavenJArray();
      for (PatchRequest patchRequest: patchesIfMissing) {
        patchesIfMissingArray.add(patchRequest.toJson());
      }
      ret.add("PatchesIfMissing", patchesIfMissingArray);
    }
    return ret;
  }

}
