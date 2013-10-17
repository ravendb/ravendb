package net.ravendb.client;

import net.ravendb.abstractions.basic.UseSharpEnum;

@UseSharpEnum
public enum EscapeQueryOptions {
  ESCAPE_ALL,

  ALLOW_POSTFIX_WILDCARD,

  /**
   * This allows queries such as Name:*term*, which tend to be much
   * more expensive and less performant than any other queries.
   * Consider carefully whatever you really need this, as there are other
   * alternative for searching without doing extremely expensive leading
   * wildcard matches.
   */
  ALLOW_ALL_WILDCARDS,
  RAW_QUERY;
}
