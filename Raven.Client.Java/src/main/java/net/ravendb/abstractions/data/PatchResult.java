package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.UseSharpEnum;

@UseSharpEnum
public enum PatchResult {
  /**
   * The document does not exists, operation was a no-op
   */
  DOCUMENT_DOES_NOT_EXISTS,
  /**
   * Document was properly patched
   */
  PATCHED,
  /**
   * Document was properly tested
   */
  TESTED;
}
