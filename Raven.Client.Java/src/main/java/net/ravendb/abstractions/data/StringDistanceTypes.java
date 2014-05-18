package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.UseSharpEnum;

/**
 * String distance algorithms used in suggestion query
 *
 */
@UseSharpEnum
public enum StringDistanceTypes {
  /**
   * Default, suggestion is not active
   */
  NONE,

  /**
   *  Default, equivalent to Levenshtein
   */
  DEFAULT,
  /**
   * Levenshtein distance algorithm (default)
   */
  LEVENSHTEIN,

  /**
   * JaroWinkler distance algorithm
   */
  JARO_WINKLER,

  /**
   * NGram distance algorithm
   */
  N_GRAM;
}
