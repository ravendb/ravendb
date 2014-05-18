package net.ravendb.client.document.batches;

import java.util.Collection;
import java.util.UUID;

import net.ravendb.abstractions.basic.Lazy;

import com.mysema.query.types.Path;

/**
 * Fluent interface for specifying include paths
 * for loading documents lazily
 * @param <T>
 * NOTE: Java version does not contain method load that skips class parameter - since we can't track type in method signature based on Path object
 */
public interface ILazyLoaderWithInclude {

  /**
   * Includes the specified path.
   * @param path
   * @return
   */
  public ILazyLoaderWithInclude include(String path);

  /**
   * Includes the specified path
   * @param path
   * @return
   */
  public ILazyLoaderWithInclude include(Path<?> path);

  /**
   * Loads the specified ids.
   * @param clazz
   * @param ids
   * @return
   */
  public <TResult> Lazy<TResult[]> load(Class<TResult> clazz, String... ids);

  /**
   * Loads the specified ids.
   * @param ids
   * @return
   */
  public <TResult> Lazy<TResult[]> load(Class<TResult> clazz, Collection<String> ids);

  /**
   * Loads the specified id.
   * @param clazz
   * @param id
   * @return
   */
  public <TResult> Lazy<TResult> load(Class<TResult> clazz, String id);

  /**
   * Loads the specified entity with the specified id after applying
   * conventions on the provided id to get the real document id.
   *
   * This method allows you to call:
   * lazyLoad(Post.class, 1)
   * And that call will internally be translated to
   * lazyLoad(Post.class, "posts/1")
   * @param clazz
   * @param id
   * @return
   */
  public <TResult> Lazy<TResult> load(Class<TResult> clazz, Number id);

  /**
   * Loads the specified entity with the specified id after applying
   * conventions on the provided id to get the real document id.
   *
   * This method allows you to call:
   * lazyLoad(Post.class, 1)
   * And that call will internally be translated to
   * lazyLoad(Post.class, "posts/1")
   * @param clazz
   * @param id
   * @return
   */
  public <TResult> Lazy<TResult> load(Class<TResult> clazz, UUID id);

  /**
   * Loads the specified entities with the specified id after applying
   * conventions on the provided id to get the real document id.
   *
   * This method allows you to call:
   * lazyLoad(Post.class, 1,2,3);
   * And that call will internally be translated to
   * lazyLoad(Post.class, "posts/1", "posts/2", "posts/3")
   *
   * Or whatever your conventions specify.
   */
  public <TResult> Lazy<TResult[]> load(Class<TResult> clazz, Number... ids);

  /**
   * Loads the specified entities with the specified id after applying
   * conventions on the provided id to get the real document id.
   *
   * This method allows you to call:
   * lazyLoad(Post.class, 1,2,3);
   * And that call will internally be translated to
   * lazyLoad(Post.class, "posts/1", "posts/2", "posts/3")
   *
   * Or whatever your conventions specify.
   */
  public <TResult> Lazy<TResult[]> load(Class<TResult> clazz, UUID... ids);

}
