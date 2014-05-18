package net.ravendb.client.document;

import java.util.Collection;
import java.util.UUID;

import com.mysema.query.types.Path;

/**
 * Fluent interface for specifying include paths
 * for loading documents
 *
 * @param <T>
 */
public interface ILoaderWithInclude {

  /**
   * Includes the specified path.
   * @param path
   * @return
   */
  public ILoaderWithInclude include(String path);

  /**
   * Includes the specified path.
   * @param path
   * @return
   */
  public ILoaderWithInclude include(Path<?> path);

  /**
   * Includes the specified path.
   * @param path
   * @return
   */
  public ILoaderWithInclude include(Class<?> targetClass, Path<?> path);

  /**
   * Loads the specified ids.
   * @param clazz
   * @param ids
   * @return
   */
  public <TResult> TResult[] load(Class<TResult> clazz, String... ids);

  /**
   * Loads the specified ids.
   * @param clazz
   * @param ids
   * @return
   */
  public <TResult> TResult[] load(Class<TResult> clazz, Collection<String> ids);

  /**
   * Loads the specified id.
   * @param clazz
   * @param id
   * @return
   */
  public <TResult> TResult load(Class<TResult> clazz, String id);

  /**
   * Loads the specified entity with the specified id after applying
   * conventions on the provided id to get the real document id.
   *
   * This method allows you to call:
   * load(Post.class, 1)
   * And that call will internally be translated to
   * load(Post.class, "posts/1");
   *
   * Or whatever your conventions specify.
   * @param clazz
   * @param id
   * @return
   */
  public <TResult> TResult load(Class<TResult> clazz, Number id);

  /**
   * Loads the specified entity with the specified id after applying
   * conventions on the provided id to get the real document id.
   *
   * This method allows you to call:
   * load(Post.class, 1)
   * And that call will internally be translated to
   * load(Post.class, "posts/1");
   *
   * Or whatever your conventions specify.
   * @param clazz
   * @param id
   * @return
   */
  public <TResult> TResult load(Class<TResult> clazz, UUID id);

  /**
   * Loads the specified entity with the specified id after applying
   * conventions on the provided id to get the real document id.
   *
   * This method allows you to call:
   * load(Post.class, 1, 2, 3)
   * And that call will internally be translated to
   * load(Post.class, "posts/1", "posts/2", "posts/3");
   *
   * Or whatever your conventions specify.
   * @param clazz
   * @param id
   * @return
   */
  public <TResult> TResult[] load(Class<TResult> clazz, UUID... ids);

  /**
   * Loads the specified entity with the specified id after applying
   * conventions on the provided id to get the real document id.
   *
   * This method allows you to call:
   * load(Post.class, 1, 2, 3)
   * And that call will internally be translated to
   * load(Post.class, "posts/1", "posts/2", "posts/3");
   *
   * Or whatever your conventions specify.
   * @param clazz
   * @param id
   * @return
   */
  public <TResult> TResult[] load(Class<TResult> clazz, Number... ids);


}
