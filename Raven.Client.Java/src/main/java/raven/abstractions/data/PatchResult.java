package raven.abstractions.data;

import raven.abstractions.basic.UseSharpEnum;

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
