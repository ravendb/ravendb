package raven.client.document;

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
  MONOTONIC_READ,
  /**
   * After updating a documents, will only accept queries which already indexed the updated value.
   */
  QUERY_YOUR_WRITES,
}
