package raven.client.document.batches;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import com.google.common.base.Defaults;
import com.mysema.query.types.Path;

import raven.abstractions.basic.Lazy;
import raven.abstractions.basic.Tuple;
import raven.abstractions.closure.Function0;
import raven.abstractions.extensions.ExpressionExtensions;
import raven.client.IDocumentSessionImpl;

public class LazyMultiLoaderWithInclude implements ILazyLoaderWithInclude {
  private final IDocumentSessionImpl session;
  private final List<Tuple<String, Class<?>>> includes = new ArrayList<>();

  /**
   * Initializes a new instance of the {@link LazyMultiLoaderWithInclude} class.
   * @param session
   */
  public LazyMultiLoaderWithInclude(IDocumentSessionImpl session) {
    this.session = session;
  }

  /**
   * Includes the specified path.
   * @param path
   * @return
   */
  public ILazyLoaderWithInclude lazyInclude(String path) {
    includes.add(Tuple.<String, Class<?>> create(path, Object.class));
    return this;
  }

  /**
   * Includes the specified path.
   * @param path
   * @return
   */
  public ILazyLoaderWithInclude lazyInclude(Path<?> path) {
    return lazyInclude(ExpressionExtensions.toPropertyPath(path));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <TResult> Lazy<TResult[]> lazyLoad(Class<TResult> clazz, String... ids) {
    return session.lazyLoadInternal(clazz, ids, includes.toArray(new Tuple[0]), null);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <TResult> Lazy<TResult[]> lazyLoad(Class<TResult> clazz, Collection<String> ids) {
    return session.lazyLoadInternal(clazz, ids.toArray(new String[0]), includes.toArray(new Tuple[0]), null);
  }

  @Override
  public <TResult> Lazy<TResult> lazyLoad(final Class<TResult> clazz, String id) {
    final Lazy<TResult[]> lazy = lazyLoad(clazz, Arrays.asList(id));
    return new Lazy<TResult>(new Function0<TResult>() {
      @Override
      public TResult apply() {
        TResult[] results = lazy.getValue();
        return results.length > 0 ? results[0] : Defaults.defaultValue(clazz);
      }
    });
  }

  @Override
  public <TResult> Lazy<TResult> lazyLoad(Class<TResult> clazz, Number id) {
    String documentKey = session.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false);
    return lazyLoad(clazz, documentKey);
  }

  @Override
  public <TResult> Lazy<TResult> lazyLoad(Class<TResult> clazz, UUID id) {
    String documentKey = session.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false);
    return lazyLoad(clazz, documentKey);
  }

  @Override
  public <TResult> Lazy<TResult[]> lazyLoad(Class<TResult> clazz, Number... ids) {
    List<String> documentKeys = new ArrayList<>();
    for (Number id: ids) {
      documentKeys.add(session.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false));
    }
    return lazyLoad(clazz, documentKeys);
  }

  @Override
  public <TResult> Lazy<TResult[]> lazyLoad(Class<TResult> clazz, UUID... ids) {
    List<String> documentKeys = new ArrayList<>();
    for (UUID id: ids) {
      documentKeys.add(session.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false));
    }
    return lazyLoad(clazz, documentKeys);
  }




}
