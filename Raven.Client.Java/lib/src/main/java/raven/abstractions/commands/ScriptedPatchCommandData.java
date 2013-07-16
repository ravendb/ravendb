package raven.abstractions.commands;

import raven.abstractions.basic.SharpEnum;
import raven.abstractions.data.Etag;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.data.ScriptedPatchRequest;
import raven.abstractions.data.TransactionInformation;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJValue;

/**
 * A single batch operation for a document EVAL (using a Javascript)
 */
public class ScriptedPatchCommandData implements ICommandData {
  private ScriptedPatchRequest patch;
  private ScriptedPatchRequest patchIfMissing;
  private String key;
  private Etag etag;
  private TransactionInformation transactionInformation;
  private RavenJObject metadata;
  private boolean debugMode;
  private RavenJObject additionalData;

  public HttpMethods getMethod() {
    return HttpMethods.EVAL;
  }

  public ScriptedPatchRequest getPatch() {
    return patch;
  }

  public void setPatch(ScriptedPatchRequest patch) {
    this.patch = patch;
  }

  public ScriptedPatchRequest getPatchIfMissing() {
    return patchIfMissing;
  }

  public void setPatchIfMissing(ScriptedPatchRequest patchIfMissing) {
    this.patchIfMissing = patchIfMissing;
  }

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

  public boolean isDebugMode() {
    return debugMode;
  }

  public void setDebugMode(boolean debugMode) {
    this.debugMode = debugMode;
  }

  public RavenJObject getAdditionalData() {
    return additionalData;
  }

  public void setAdditionalData(RavenJObject additionalData) {
    this.additionalData = additionalData;
  }

  public TransactionInformation getTransactionInformation() {
    return transactionInformation;
  }

  public void setTransactionInformation(TransactionInformation transactionInformation) {
    this.transactionInformation = transactionInformation;
  }

  @Override
  public RavenJObject toJson() {
    RavenJObject ret = new RavenJObject();
    ret.add("Key", new RavenJValue(key));
    ret.add("Method", RavenJValue.fromObject(SharpEnum.value(getMethod())));

    RavenJObject patch = new RavenJObject();
    patch.add("Script", new RavenJValue(this.patch.getScript()));
    patch.add("Values", RavenJObject.fromObject(this.patch.getValues()));

    ret.add("Patch", patch);
    ret.add("DebugMode", new RavenJValue(debugMode));
    ret.add("AdditionalData", additionalData);

    if (etag != null) {
      ret.add("Etag", new RavenJValue(etag.toString()));
    }
    if (patchIfMissing != null) {
      RavenJObject patchIfMissing = new RavenJObject();
      patchIfMissing.add("Script", new RavenJValue(this.patchIfMissing.getScript()));
      patchIfMissing.add("Values", RavenJObject.fromObject(this.patchIfMissing.getValues()));
      ret.add("PatchIfMissing", patchIfMissing);
    }
    return ret;
  }

}