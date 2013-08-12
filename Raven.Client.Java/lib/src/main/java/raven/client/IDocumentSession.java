package raven.client;

import java.util.Collection;
import java.util.UUID;

import com.mysema.query.types.Path;

import raven.abstractions.data.Etag;
import raven.client.document.ILoaderWithInclude;

/**
 * Interface for document session
 */
public interface IDocumentSession extends AutoCloseable {

  /**
   * Get the accessor for advanced operations
   *
   * Those operations are rarely needed, and have been moved to a separate
   * property to avoid cluttering the API
   * @return
   */
  public ISyncAdvancedSessionOperation advanced();

  /**
   * Marks the specified entity for deletion. The entity will be deleted when IDocumentSession.SaveChanges is called.
   * @param entity
   */
  public <T> void delete(T entity);

  /**
   * Loads the specified entity with the specified id.
   * @param clazz
   * @param id
   * @return
   */
  public <T> T load(Class<T> clazz, String id);

  /**
   * Loads the specified entities with the specified ids.
   * @param clazz
   * @param ids
   * @return
   */
  public <T> T[] load(Class<T> clazz, String...ids);

  /**
   * Loads the specified entities with the specified ids.
   * @param clazz
   * @param ids
   * @return
   */
  public <T> T[] load(Class<T> clazz, Collection<String> ids);

  /**
   * Loads the specified entity with the specified id after applying
   * conventions on the provided id to get the real document id.
   *
   * This method allows you to call:
   * Load(Post.class, 1)
   *
   * And that call will internally be translated to
   * Load(Post.class, "posts/1");
   *
   * Or whatever your conventions specify.
   * @param clazz
   * @param id
   * @return
   */
  public <T> T load(Class<T> clazz, Number id);

  /**
   * Loads the specified entity with the specified id after applying
   * conventions on the provided id to get the real document id.
   *
   * This method allows you to call:
   * Load(Post.class, 1)
   *
   * And that call will internally be translated to
   * Load(Post.class, "posts/1");
   *
   * Or whatever your conventions specify.
   * @param clazz
   * @param id
   * @return
   */
  public <T> T load(Class<T> clazz, UUID id);

  /**
   * Loads the specified entities with the specified id after applying
   * conventions on the provided id to get the real document id.
   *
   * This method allows you to call:
   * Load(Post.class, 1,2,3)
   * And that call will internally be translated to
   * Load(Post.class, "posts/1","posts/2","posts/3");
   *
   * Or whatever your conventions specify.
   * @param clazz
   * @param ids
   * @return
   */
  public <T> T[] load(Class<T> clazz, Number... ids);

  /**
   * Loads the specified entities with the specified id after applying
   * conventions on the provided id to get the real document id.
   *
   * This method allows you to call:
   * Load(Post.class, 1,2,3)
   * And that call will internally be translated to
   * Load(Post.class, "posts/1","posts/2","posts/3");
   *
   * Or whatever your conventions specify.
   * @param clazz
   * @param ids
   * @return
   */
  public <T> T[] load(Class<T> clazz, UUID... ids);

  /**
   * Queries the specified index.
   * @param clazz
   * @param indexName Name of the index.
   * @return
   */
  //TODO: public <T> IRavenQueryable<T> query(Class<T> clazz, String indexName);

  /**
   * Queries the specified index.
   * @param clazz
   * @param indexName Name of the index.
   * @param Whatever we are querying a map/reduce index (modify how we treat identifier properties)
   * @return
   */
//TODO: public <T> IRavenQueryable<T> query(Class<T> clazz, String indexName, boolean isMapReduce);

  /**
   * Dynamically queries RavenDB.
   * @param clazz
   * @return
   */
//TODO: public <T> IRavenQueryable<T> query(Class<T> clazz);

  /**
   * Queries the index specified by indexCreator.
   * @param clazz
   * @param indexCreator
   * @return
   */
//TODO: public <T> IRavenQueryable<T> query(Class<T> clazz, Class<AbstractIndexCreationTask> indexCreator);

  /**
   * Begin a load while including the specified path
   * @param path
   * @return
   */
  public ILoaderWithInclude include(String path);

  /**
   * Begin a load while including the specified path
   * @param path
   * @return
   */
   public ILoaderWithInclude include(Path<?> path);

  /* TODO:
    /// <summary>
    /// Performs a load that will use the specified results transformer against the specified id
    /// </summary>
    /// <typeparam name="TTransformer">The transformer to use in this load operation</typeparam>
    /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
    /// <returns></returns>
    TResult Load<TTransformer, TResult>(string id) where TTransformer : AbstractTransformerCreationTask, new();

      /// <summary>
      /// Performs a load that will use the specified results transformer against the specified id
      /// </summary>
      /// <typeparam name="TTransformer">The transformer to use in this load operation</typeparam>
      /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
      /// <param name="id"></param>
      /// <param name="configure"></param>
      /// <returns></returns>
      TResult Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure) where TTransformer : AbstractTransformerCreationTask, new();

  /// <summary>
  /// Performs a load that will use the specified results transformer against the specified id
  /// </summary>
  /// <typeparam name="TTransformer">The transformer to use in this load operation</typeparam>
  /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
  /// <returns></returns>
  TResult[] Load<TTransformer, TResult>(params string[] ids) where TTransformer : AbstractTransformerCreationTask, new();

  /// <summary>
  /// Performs a load that will use the specified results transformer against the specified id
  /// </summary>
  TResult[] Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure) where TTransformer : AbstractTransformerCreationTask, new();
   */
  /**
   * Saves all the changes to the Raven server.
   */
  public void saveChanges();

  /**
   * Stores the specified dynamic entity.
   * @param entity
   */
  public void store(Object entity);

  /**
   *  Stores the specified dynamic entity, under the specified id
   * @param entity
   * @param id The id to store this entity under. If other entity exists with the same id it will be overridden
   */
  public void store(Object entity, String id);

  /**
   * Stores the specified entity with the specified etag
   * @param entity
   * @param etag
   */
  public void store(Object entity, Etag etag);

  /**
   * Stores the specified entity with the specified etag, under the specified id
   * @param entity
   * @param etag
   * @param id
   */
  public void store(Object entity, Etag etag, String id);

}
