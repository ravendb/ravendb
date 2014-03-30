package net.ravendb.client.linq;

import net.ravendb.abstractions.basic.UseSharpEnum;

/**
 * Different query types
 */
@UseSharpEnum
public enum SpecialQueryType {
  /**
   *
   */
  NONE,

  /**
   *
   */
  ALL,

  /**
   *
   */
  ANY,

  /**
   * Get count of items for the query
   */
  COUNT,

  /**
   * Get count of items for the query as an Int64
   */
  LONG_COUNT,

  /**
   * Get only the first item
   */
  FIRST,

  /**
   * Get only the first item (or null)
   */
  FIRST_OR_DEFAULT,

  /**
   * Get only the first item (or throw if there are more than one)
   */
  SINGLE,

  /**
   * Get only the first item (or throw if there are more than one) or null if empty
   */
  SINGLE_OR_DEFAULT;


}
