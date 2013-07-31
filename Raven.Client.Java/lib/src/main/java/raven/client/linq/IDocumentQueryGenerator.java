package raven.client.linq;

import raven.client.document.DocumentConvention;

/**
 * Generate a new document query
 */
public interface IDocumentQueryGenerator {
  /**
   * Gets the conventions associated with this query
   * @return
   */
  public DocumentConvention getConventions();

  /*TODO might cause conflict with IDocumentSession.Query()
  /// <summary>
  /// Create a new query for <typeparam name="T"/>
  /// </summary>
  IDocumentQuery<T> Query<T>(string indexName, bool isMapReduce);
  */


}
