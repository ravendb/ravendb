package net.ravendb.client.linq;

import net.ravendb.client.IDocumentQuery;
import net.ravendb.client.document.DocumentConvention;

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
  public <T> IDocumentQuery<T> documentQuery(Class<T> clazz, String indexName, boolean isMapReduce);


}
