package raven.abstractions.data;

import raven.abstractions.json.linq.RavenJObject;

public class PatchResultData {
  private PatchResult patchResult;
  private RavenJObject document;

  public RavenJObject getDocument() {
    return document;
  }
  public PatchResult getPatchResult() {
    return patchResult;
  }
  public void setDocument(RavenJObject document) {
    this.document = document;
  }
  public void setPatchResult(PatchResult patchResult) {
    this.patchResult = patchResult;
  }

}
