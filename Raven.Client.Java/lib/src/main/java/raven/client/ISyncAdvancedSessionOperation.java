package raven.client;

import raven.client.document.batches.IEagerSessionOperations;
import raven.client.document.batches.ILazySessionOperations;

/**
 * Advanced synchronous session operations
 */
public interface ISyncAdvancedSessionOperation extends IAdvancedDocumentSessionOperations {
  /**
   * Refreshes the specified entity from Raven server.
   * @param entity
   */
  public <T> void refresh(T entity);

  /**
   * Load documents with the specified key prefix
   * @param clazz
   * @param keyPrefix
   * @return
   */
  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix);

  /**
   * Load documents with the specified key prefix
   * @param clazz
   * @param keyPrefix
   * @param matches
   * @return
   */
  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix, String matches);

  /**
   * Load documents with the specified key prefix
   * @param clazz
   * @param keyPrefix
   * @param matches
   * @param start
   * @return
   */
  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start);

  /**
   * Load documents with the specified key prefix
   * @param clazz
   * @param keyPrefix
   * @param matches
   * @param start
   * @param pageSize
   * @return
   */
  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start, int pageSize);


  /**
   * Access the lazy operations
   * @return
   */
  public ILazySessionOperations lazily();

  /**
   * Access the eager operations
   * @return
   */
  public IEagerSessionOperations eagerly();

  /*TODO
  /// <summary>
  /// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
  /// </summary>
  /// <typeparam name="T">The result of the query</typeparam>
  /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
  /// <returns></returns>
  IDocumentQuery<T> LuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new();

  /// <summary>
  /// Query the specified index using Lucene syntax
  /// </summary>
  /// <param name="indexName">Name of the index.</param>
  /// <param name="isMapReduce">Control how we treat identifier properties in map/reduce indexes</param>
  IDocumentQuery<T> LuceneQuery<T>(string indexName, bool isMapReduce = false);

  /// <summary>
  /// Dynamically query RavenDB using Lucene syntax
  /// </summary>
  IDocumentQuery<T> LuceneQuery<T>();*/

  /**
   * Gets the document URL for the specified entity.
   * @param entity
   * @return
   */
  public String getDocumentUrl(Object entity);

  /*TODO
  /// <summary>
  /// Stream the results on the query to the client, converting them to
  /// CLR types along the way.
  /// Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called
  /// </summary>
  IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query);
  /// <summary>
  /// Stream the results on the query to the client, converting them to
  /// CLR types along the way.
  /// Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called
  /// </summary>
  IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query, out QueryHeaderInformation queryHeaderInformation);

  /// <summary>
  /// Stream the results on the query to the client, converting them to
  /// CLR types along the way.
  /// Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called
  /// </summary>
  IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query);

  /// <summary>
  /// Stream the results on the query to the client, converting them to
  /// CLR types along the way.
  /// Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called
  /// </summary>
  IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query, out QueryHeaderInformation queryHeaderInformation);

  /// <summary>
  /// Stream the results of documents searhcto the client, converting them to CLR types along the way.
  /// Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called
  /// </summary>
  IEnumerator<StreamResult<T>> Stream<T>(Etag fromEtag = null, string startsWith = null, string matches = null,
                                          int start = 0, int pageSize = int.MaxValue);*/
}
