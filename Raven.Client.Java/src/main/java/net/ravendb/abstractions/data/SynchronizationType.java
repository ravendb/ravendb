package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.UseSharpEnum;

@UseSharpEnum
public enum SynchronizationType {
  UNKNOWN,
  CONTENT_UPDATE,
  METADATA_UPDATE,
  RENAME,
  DELETE;
}
