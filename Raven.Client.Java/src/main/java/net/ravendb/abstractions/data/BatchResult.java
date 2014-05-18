package net.ravendb.abstractions.data;

import net.ravendb.abstractions.json.linq.RavenJObject;

/**
 * The result of a single operation inside a batch
 */
public class BatchResult {
  private Etag etag;
  private String method;
  private String key;
  private RavenJObject metadata;
  private RavenJObject additionalData;
  private PatchResult patchResult;
  private Boolean deleted;

  public RavenJObject getAdditionalData() {
    return additionalData;
  }
  public Boolean getDeleted() {
    return deleted;
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
  public String getMethod() {
    return method;
  }
  public PatchResult getPatchResult() {
    return patchResult;
  }
  public void setAdditionalData(RavenJObject additionalData) {
    this.additionalData = additionalData;
  }
  public void setDeleted(Boolean deleted) {
    this.deleted = deleted;
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
  public void setMethod(String method) {
    this.method = method;
  }
  public void setPatchResult(PatchResult patchResult) {
    this.patchResult = patchResult;
  }

}
