package net.ravendb.client;

import java.util.Iterator;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.FacetQuery;
import net.ravendb.abstractions.data.FacetResults;
import net.ravendb.abstractions.data.QueryHeaderInformation;
import net.ravendb.abstractions.data.StreamResult;
import net.ravendb.client.document.batches.IEagerSessionOperations;
import net.ravendb.client.document.batches.ILazySessionOperations;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.client.indexes.AbstractTransformerCreationTask;
import net.ravendb.client.linq.IRavenQueryable;


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
   * Load documents with the specified key prefix
   * @param clazz
   * @param keyPrefix
   * @param matches
   * @param start
   * @param pageSize
   * @return
   */
  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start, int pageSize, String exclude);

  /**
   * Load documents with the specified key prefix
   * @param clazz
   * @param keyPrefix
   * @param matches
   * @param start
   * @param pageSize
   * @return
   */
  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start, int pageSize, String exclude, RavenPagingInformation pagingInformation);


  /**
   * Loads documents with the specified key prefix and applies the specified results transformer against the results
   * @param clazz
   * @param transformerClass
   * @param keyPrefix
   * @param matches
   * @param start
   * @param pageSize
   * @param exclude
   * @param pagingInformation
   * @param configure
   * @return
   */
  public <TResult, TTransformer extends AbstractTransformerCreationTask> TResult[] loadStartingWith(Class<TResult> clazz, Class<TTransformer> transformerClass,
    String keyPrefix, String matches, int start, int pageSize, String exclude,
    RavenPagingInformation pagingInformation, Action1<ILoadConfiguration> configure);


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

  /**
   * Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
   * @param clazz The result class of the query.
   * @param indexClass The type of the index creator.
   * @return
   */
  public <T, S extends AbstractIndexCreationTask> IDocumentQuery<T> documentQuery(Class<T> clazz, Class<S> indexClass);

  /**
   * Query the specified index using Lucene syntax
   * @param indexName Name of the index.
   * @param isMapReduce Control how we treat identifier properties in map/reduce indexes
   * @return
   */
  public <T> IDocumentQuery<T> documentQuery(Class<T> clazz, String indexName, boolean isMapReduce);

  /**
   * Query the specified index using Lucene syntax
   * @param indexName Name of the index.
   * @param isMapReduce Control how we treat identifier properties in map/reduce indexes
   * @return
   */
  public <T> IDocumentQuery<T> documentQuery(Class<T> clazz, String indexName);


  /**
   * Dynamically query RavenDB using Lucene syntax
   * @param clazz
   * @return
   */
  public <T> IDocumentQuery<T> documentQuery(Class<T> clazz);

  /**
   * Gets the document URL for the specified entity.
   * @param entity
   * @return
   */
  public String getDocumentUrl(Object entity);

  /**
   * Stream the results on the query to the client, converting them to
   * Java types along the way.
   * Does NOT track the entities in the session, and will not includes changes there when saveChanges() is called
   * @param query
   * @return
   */
  public <T> Iterator<StreamResult<T>> stream(IRavenQueryable<T> query);

  /**
   * Stream the results on the query to the client, converting them to
   * Java types along the way.
   * Does NOT track the entities in the session, and will not includes changes there when saveChanges() is called
   * @param query
   * @param queryHeaderInformation
   * @return
   */
  public <T> Iterator<StreamResult<T>> stream(IRavenQueryable<T> query, Reference<QueryHeaderInformation> queryHeaderInformation);


  /**
   * Stream the results on the query to the client, converting them to
   * Java types along the way.
   * Does NOT track the entities in the session, and will not includes changes there when saveChanges() is called
   * @param query
   * @return
   */
  public <T> Iterator<StreamResult<T>> stream(IDocumentQuery<T> query);

  /**
   * Stream the results on the query to the client, converting them to
   * Java types along the way.
   * Does NOT track the entities in the session, and will not includes changes there when saveChanges() is called
   * @param query
   * @return
   */
  public <T> Iterator<StreamResult<T>> stream(IDocumentQuery<T> query, Reference<QueryHeaderInformation> queryHeaderInformation);

  /**
   * Stream the results on the query to the client, converting them to
   * Java types along the way.
   * Does NOT track the entities in the session, and will not includes changes there when saveChanges() is called
   * @param fromEtag
   * @return
   */
  public <T> Iterator<StreamResult<T>> stream(Class<T> entityClass);


  /**
   * Stream the results on the query to the client, converting them to
   * Java types along the way.
   * Does NOT track the entities in the session, and will not includes changes there when saveChanges() is called
   * @param fromEtag
   * @return
   */
  public <T> Iterator<StreamResult<T>> stream(Class<T> entityClass, Etag fromEtag);

  /**
   * Stream the results on the query to the client, converting them to
   * Java types along the way.
   * Does NOT track the entities in the session, and will not includes changes there when saveChanges() is called
   * @param fromEtag
   * @param startsWith
   * @return
   */
  public <T> Iterator<StreamResult<T>> stream(Class<T> entityClass, Etag fromEtag, String startsWith);

  /**
   * Stream the results on the query to the client, converting them to
   * Java types along the way.
   * Does NOT track the entities in the session, and will not includes changes there when saveChanges() is called
   * @param fromEtag
   * @param startsWith
   * @param matches
   * @return
   */
  public <T> Iterator<StreamResult<T>> stream(Class<T> entityClass, Etag fromEtag, String startsWith, String matches);

  /**
   * Stream the results on the query to the client, converting them to
   * Java types along the way.
   * Does NOT track the entities in the session, and will not includes changes there when saveChanges() is called
   * @param fromEtag
   * @param startsWith
   * @param matches
   * @param start
   * @return
   */
  public <T> Iterator<StreamResult<T>> stream(Class<T> entityClass, Etag fromEtag, String startsWith, String matches, int start);

  /**
   * Stream the results on the query to the client, converting them to
   * Java types along the way.
   * Does NOT track the entities in the session, and will not includes changes there when saveChanges() is called
   * @param fromEtag
   * @param startsWith
   * @param matches
   * @param start
   * @param pageSize
   * @return
   */
  public <T> Iterator<StreamResult<T>> stream(Class<T> entityClass, Etag fromEtag, String startsWith, String matches, int start, int pageSize);

  /**
   * Stream the results on the query to the client, converting them to
   * Java types along the way.
   * Does NOT track the entities in the session, and will not includes changes there when saveChanges() is called
   * @param fromEtag
   * @param startsWith
   * @param matches
   * @param start
   * @param pageSize
   * @return
   */
  public <T> Iterator<StreamResult<T>> stream(Class<T> entityClass, Etag fromEtag, String startsWith, String matches, int start, int pageSize, RavenPagingInformation pagingInformation);

  /**
   *
   * @param FacetQuery
   * @return
   */
  public FacetResults[] multiFacetedSearch(FacetQuery... queries);

}
