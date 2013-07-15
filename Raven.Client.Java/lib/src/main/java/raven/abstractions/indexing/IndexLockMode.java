package raven.abstractions.indexing;


import raven.abstractions.basic.UseSharpEnum;

@UseSharpEnum
public enum IndexLockMode {
  UNLOCK,
  LOCKED_IGNORE,
  LOCKED_ERROR;

}
