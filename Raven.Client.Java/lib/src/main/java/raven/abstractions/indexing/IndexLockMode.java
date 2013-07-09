package raven.abstractions.indexing;

import org.codehaus.jackson.annotate.JsonCreator;

import raven.abstractions.basic.SharpEnum;

public enum IndexLockMode {
  UNLOCK,
  LOCKED_IGNORE,
  LOCKED_ERROR;

  @JsonCreator
  public static IndexLockMode fromValue(String v) {
    return SharpEnum.fromValue(v, IndexLockMode.class);
  }
}
