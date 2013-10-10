package raven.client.listeners;

import raven.client.IDocumentQueryCustomization;

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
