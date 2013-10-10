package raven.client.linq;

import raven.client.IDocumentQuery;
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

  /**
   * Create a new query for
   */
  public <T> IDocumentQuery<T> luceneQuery(Class<T> clazz, String indexName, boolean isMapReduce);


}
