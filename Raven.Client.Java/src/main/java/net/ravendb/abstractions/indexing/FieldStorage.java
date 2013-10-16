package net.ravendb.abstractions.indexing;

import net.ravendb.abstractions.basic.UseSharpEnum;

/**
 *  Specifies whether and how a field should be stored.
 */
@UseSharpEnum
public enum FieldStorage {
  /**
   *  Store the original field value in the index. This is useful for short texts like a document's title which should be displayed with the results.
   *  The value is stored in its original form, i.e. no analyzer is used before it is stored.
   */
  YES,
  /**
   * Do not store the field value in the index.
   */
  NO;
}
