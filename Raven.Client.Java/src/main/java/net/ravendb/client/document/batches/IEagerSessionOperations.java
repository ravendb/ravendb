package net.ravendb.client.document.batches;

import net.ravendb.client.document.ResponseTimeInformation;

/**
 * Allow to perform eager operations on the session
 */
public interface IEagerSessionOperations {
  /**
   * Execute all the lazy requests pending within this session
   */
  ResponseTimeInformation executeAllPendingLazyOperations();
}
