package raven.client.json;

/**
 * Specified the type of token.
 *
 */
public enum JTokenType {

  /**
   * No token type has been set
   */
  NONE,
  /**
   * A JSON object
   */
  OBJECT,
  /**
   * a JSON array
   */
  ARRAY,
  /**
   * A JSON constructor
   */
  CONSTRUCTOR,

  /**
   * A JSON object property
   */
  PROPERTY,

  /**
   * A comment.
   */
  COMMENT,
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
   * An undefined value
   */
  UNDEFINED,
  /**
   * A date value.
   */
  DATE,
  /**
   * A raw JSON value.
   */
  RAW,
  /**
   * A collection of bytes value.
   */
  BYTES,
  /**
   * A guid value.
   */
  GUID,
  /**
   * A Uri value.
   */
  URI,
  /**
   * A TimeSpan value.
   */
  TIMESPAN; //TODO: do we provide support for this type ?


}
