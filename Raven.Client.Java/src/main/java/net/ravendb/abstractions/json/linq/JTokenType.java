package net.ravendb.abstractions.json.linq;

/**
 * Specified the type of token.
 *
 */
public enum JTokenType {

  /**
   * A JSON object
   */
  OBJECT,
  /**
   * a JSON array
   */
  ARRAY,
  /**
   * An integer value.
   */
  INTEGER,
  /**
   * A float value.
   */
  FLOAT,
  /**
   * a string value.
   */
  STRING,
  /**
   * A boolean value.
   */
  BOOLEAN,
  /**
   * A null value.
   */
  NULL,
  /**
   * A collection of bytes value.
   */
  BYTES,
  /**
   * A date value.
   */
  DATE;

}
