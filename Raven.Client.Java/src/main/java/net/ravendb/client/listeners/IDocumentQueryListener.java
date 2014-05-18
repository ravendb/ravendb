package net.ravendb.client.listeners;

import net.ravendb.client.IDocumentQueryCustomization;

/**
 * Hook for users to modify all queries globally
 */
public interface IDocumentQueryListener {
  /**
   * Allow to customize a query globally
   * @param queryCustomization
   */
  void beforeQueryExecuted(IDocumentQueryCustomization queryCustomization);
}
