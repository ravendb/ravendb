package raven.abstractions.data;

/**
 * String distance algorithms used in suggestion query
 *
 */
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
