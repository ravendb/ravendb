package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.UseSharpEnum;

@UseSharpEnum
public enum HttpMethods {
  /**
   * GET method
   */
  GET,
  /**
   * POST method
   */
  POST,

  /**
   * DELETE method
   */
  DELETE,

  /**
   * PUT method
   */
  PUT,

  /**
   * HEAD method
   */
  HEAD,

  /**
   * PATCH method
   */
  PATCH,

  /**
   * EVAL method
   */
  EVAL,
  /**
   * RESET method
   */
  RESET;
}

