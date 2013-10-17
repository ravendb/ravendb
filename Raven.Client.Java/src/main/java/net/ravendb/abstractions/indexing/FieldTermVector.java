package net.ravendb.abstractions.indexing;

import net.ravendb.abstractions.basic.UseSharpEnum;

/**
 * Specifies whether to include term vectors for a field
 */
@UseSharpEnum
public enum FieldTermVector {
  /**
   *  Do not store term vectors
   */
  NO,

  /**
   * Store the term vectors of each document. A term vector is a list of the document's
   * terms and their number of occurrences in that document.
   */
  YES,
  /**
   * Store the term vector + token position information
   */
  WITH_POSITIONS,

  /**
   * Store the term vector + Token offset information
   */
  WITH_OFFESTS,

  /**
   * Store the term vector + Token position and offset information
   */
  WITH_POSITIONS_AND_OFFSETS;
}
