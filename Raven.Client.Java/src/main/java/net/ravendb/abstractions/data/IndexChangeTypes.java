package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.SerializeUsingValue;
import net.ravendb.abstractions.basic.UseSharpEnum;

@UseSharpEnum
@SerializeUsingValue
public enum IndexChangeTypes {
  NONE(0),

  MAP_COMPLETED(1),
  REDUCE_COMPLETED(2),
  REMOVE_FROM_INDEX(4),

  INDEX_ADDED(8),
  INDEX_REMOVED(16),

  INDEX_DEMOTED_TO_IDLE(32),
  INDEX_PROMOTED_FROM_IDLE(64),

  INDEX_DEMOTED_TO_ABANDONED(128),

  INDEX_DEMOTED_TO_DISABLED(256),

  INDEX_MARKED_AS_ERRORED(512);

  private int value;

  private IndexChangeTypes(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
