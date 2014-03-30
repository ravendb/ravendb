package net.ravendb.client.document;

/**
 *  The consistency options for all queries, fore more details about the consistency options, see:
 *  http://www.allthingsdistributed.com/2008/12/eventually_consistent.html
 *
 *  Note that this option impact only queries, since we have Strong Consistency model for the documents
 */
public enum ConsistencyOptions {

  /**
   * Ensures that after querying an index at time T, you will never see the results
   * of the index at a time prior to T.
   * This is ensured by the server, and require no action from the client
   */
  NONE,

  /**
   *  After updating a documents, will only accept queries which already indexed the updated value.
   */
  ALWAYS_WAIT_FOR_NON_STALE_RESULTS_AS_OF_LAST_WRITE,

  /**
   * Ensures that after querying an index at time T, you will never see the results
   * of the index at a time prior to T.
   * This is ensured by the server, and require no action from the client
   *
   * Use AlwaysWaitForNonStaleResultsAsOfLastWrite, instead
   */
  @Deprecated
  MONOTONIC_READ,
  /**
   * After updating a documents, will only accept queries which already indexed the updated value.
   *
   * Use None, instead
   */
  @Deprecated
  QUERY_YOUR_WRITES,
}
