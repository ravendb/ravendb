package net.ravendb.abstractions.commands;

import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.abstractions.data.ScriptedPatchRequest;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJValue;

/**
 * A single batch operation for a document EVAL (using a Javascript)
 */
public class ScriptedPatchCommandData implements ICommandData {
  private ScriptedPatchRequest patch;
  private ScriptedPatchRequest patchIfMissing;
  private String key;
  private Etag etag;
  private RavenJObject metadata;
  private boolean debugMode;
  private RavenJObject additionalData;

  @Override
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

  @Override
  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  @Override
  public Etag getEtag() {
    return etag;
  }

  public void setEtag(Etag etag) {
    this.etag = etag;
  }

  @Override
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

  @Override
  public RavenJObject getAdditionalData() {
    return additionalData;
  }

  @Override
  public void setAdditionalData(RavenJObject additionalData) {
    this.additionalData = additionalData;
  }

  @Override
  public RavenJObject toJson() {
    RavenJObject ret = new RavenJObject();
    ret.add("Key", new RavenJValue(key));
    ret.add("Method", RavenJValue.fromObject(getMethod().name()));

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