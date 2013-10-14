package net.ravendb.abstractions.indexing;


import net.ravendb.abstractions.basic.UseSharpEnum;

@UseSharpEnum
public enum IndexLockMode {
  UNLOCK,
  LOCKED_IGNORE,
  LOCKED_ERROR;

}
