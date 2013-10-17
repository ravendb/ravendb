package net.ravendb.client;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import net.ravendb.abstractions.data.Etag;
import net.ravendb.client.document.ILoaderWithInclude;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.client.indexes.AbstractTransformerCreationTask;
import net.ravendb.client.linq.IRavenQueryable;

import com.mysema.query.types.Path;


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
  public <T> IRavenQueryable<T> query(Class<T> clazz, String indexName);

  /**
   * Queries the specified index.
   * @param clazz
   * @param indexName Name of the index.
   * @param Whatever we are querying a map/reduce index (modify how we treat identifier properties)
   * @return
   */
  public <T> IRavenQueryable<T> query(Class<T> clazz, String indexName, boolean isMapReduce);

  /**
   * Dynamically queries RavenDB.
   * @param clazz
   * @return
   */
  public <T> IRavenQueryable<T> query(Class<T> clazz);

  /**
   * Queries the index specified by indexCreator.
   * @param clazz
   * @param indexCreator
   * @return
   */
  public <T> IRavenQueryable<T> query(Class<T> clazz, Class<? extends AbstractIndexCreationTask> indexCreator);

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

  /**
   * Begin a load while include the specified path
   * @param targetEntityClass Target entity class (used for id generation)
   * @param path
   * @return
   */
  public ILoaderWithInclude include(Class<?> targetEntityClass, Path<?> path);

  /**
   * Performs a load that will use the specified results transformer against the specified id
   * @param tranformerClass The transformer to use in this load operation
   * @param clazz The results shape to return after the load operation
   * @param id
   * @return
   */
  public <TResult, TTransformer extends AbstractTransformerCreationTask> TResult load(Class<TTransformer> tranformerClass,
      Class<TResult> clazz, String id);

  /**
   * Performs a load that will use the specified results transformer against the specified id
   * @param tranformerClass The transformer to use in this load operation
   * @param clazz The results shape to return after the load operation
   * @param id
   * @param configure
   * @return
   */
  public <TResult, TTransformer extends AbstractTransformerCreationTask> TResult load(Class<TTransformer> tranformerClass,
      Class<TResult> clazz, String id, LoadConfigurationFactory configure);

  /**
   * Performs a load that will use the specified results transformer against the specified id
   * @param tranformerClass The transformer to use in this load operation
   * @param clazz The results shape to return after the load operation
   * @param ids
   * @return
   */
  public <TResult, TTransformer extends AbstractTransformerCreationTask> TResult[] load(Class<TTransformer> tranformerClass,
      Class<TResult> clazz, String... ids);

  /**
   * Performs a load that will use the specified results transformer against the specified id
   * @param tranformerClass The transformer to use in this load operation
   * @param clazz The results shape to return after the load operation
   * @param ids
   * @param configure
   * @return
   */
  public <TResult, TTransformer extends AbstractTransformerCreationTask> TResult[] load(Class<TTransformer> tranformerClass,
      Class<TResult> clazz, List<String> ids, LoadConfigurationFactory configure);

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
