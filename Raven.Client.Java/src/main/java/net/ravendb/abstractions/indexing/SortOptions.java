package net.ravendb.abstractions.indexing;

import net.ravendb.abstractions.basic.SerializeUsingValue;
import net.ravendb.abstractions.basic.UseSharpEnum;

/**
 * The sort options to use for a particular field
 *
 */
@UseSharpEnum
@SerializeUsingValue
public enum SortOptions {

  /**
   * No sort options
   */
  NONE(0),

  /**
   * Sort using term values as Strings.  Sort values are String and lower
   *  values are at the front.
   */
  STRING(3),

  /**
   * Sort using term values as encoded Integers.  Sort values are Integer and
   * lower values are at the front.
   */
  INT(4),

  /**
   * Sort using term values as encoded Floats.  Sort values are Float and
   * lower values are at the front.
   */
  FLOAT(5),

  /**
   * Sort using term values as encoded Longs.  Sort values are Long and
   * lower values are at the front.
   */
  LONG(6),

  /**
   * Sort using term values as encoded Doubles.  Sort values are Double and
   * lower values are at the front.
   */
  DOUBLE(7),

  /**
   * Sort using term values as encoded Shorts.  Sort values are Short and
   * lower values are at the front.
   */
  SHORT(8),

  /**
   * Sort using a custom Comparator.  Sort values are any Comparable and
   * sorting is done according to natural order.
   */
  CUSTOM(9),

  /**
   * Sort using term values as encoded Bytes.  Sort values are Byte and
   * lower values are at the front.
   */
  BYTE(10),

  /**
   * Sort using term values as Strings, but comparing by
   * value (using String.compareTo) for all comparisons.
   * This is typically slower than {@link String}, which
   *  uses ordinals to do the sorting.
   */
  STRING_VAL(11);

  final private int value;

  private SortOptions(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

}
