package net.ravendb.abstractions.commands;

import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.abstractions.data.PatchRequest;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJValue;

public class PatchCommandData implements ICommandData {

  private PatchRequest[] patches;
  private PatchRequest[] patchesIfMissing;
  private String key;
  private Etag etag;
  private RavenJObject metadata;
  private RavenJObject additionalData;

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
    return metadata;
  }

  @Override
  public HttpMethods getMethod() {
    return HttpMethods.PATCH;
  }

  public PatchRequest[] getPatches() {
    return patches;
  }

  public PatchRequest[] getPatchesIfMissing() {
    return patchesIfMissing;
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

  public void setMetadata(RavenJObject metadata) {
    this.metadata = metadata;
  }

  public void setPatches(PatchRequest[] patches) {
    this.patches = patches;
  }

  public void setPatchesIfMissing(PatchRequest[] patchesIfMissing) {
    this.patchesIfMissing = patchesIfMissing;
  }

  @Override
  public RavenJObject toJson() {
    RavenJObject ret = new RavenJObject();
    ret.add("Key", new RavenJValue(key));
    ret.add("Method", new RavenJValue(getMethod().name()));

    RavenJArray patchesArray = new RavenJArray();
    for (PatchRequest patchRequest: patches) {
      patchesArray.add(patchRequest.toJson());
    }
    ret.add("Patches", patchesArray);
    ret.add("Metadata", metadata);
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
