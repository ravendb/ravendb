package raven.client.document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import raven.abstractions.basic.Lazy;
import raven.abstractions.basic.Tuple;
import raven.abstractions.closure.Action1;
import raven.abstractions.closure.Function0;
import raven.client.document.batches.ILazyLoaderWithInclude;
import raven.client.document.batches.ILazySessionOperations;
import raven.client.document.batches.LazyLoadOperation;
import raven.client.document.batches.LazyMultiLoaderWithInclude;
import raven.client.document.batches.LazyStartsWithOperation;
import raven.client.document.sessionoperations.LoadOperation;

import com.mysema.query.types.Path;

public class LazySessionOperations implements ILazySessionOperations {

  private DocumentSession delegate;

  public LazySessionOperations(DocumentSession delegate) {
    super();
    this.delegate = delegate;
  }

  /**
   * Begin a load while including the specified path
   */
  @Override
  public ILazyLoaderWithInclude include(Path<?> path) {
    return new LazyMultiLoaderWithInclude(delegate).include(path);
  }

  /**
   * Loads the specified ids.
   */
  @Override
  public <T> Lazy<T[]> load(Class<T> clazz, String... ids) {
    return load(clazz, Arrays.asList(ids), null);
  }

  /**
   * Loads the specified ids.
   */
  @Override
  public <T> Lazy<T[]> load(Class<T> clazz, Collection<String> ids) {
    return load(clazz, ids, null);
  }

  /**
   * Loads the specified id.
   */
  @Override
  public <T> Lazy<T> load(Class<T> clazz, String id) {
    return load(clazz, id, null);
  }


  /**
   * Loads the specified ids and a function to call when it is evaluated
   */
  @Override
  @SuppressWarnings("unchecked")
  public <TResult> Lazy<TResult[]> load(Class<TResult> clazz, Collection<String> ids, Action1<TResult[]> onEval) {
    return delegate.lazyLoadInternal(clazz, ids.toArray(new String[0]), new Tuple[0], onEval);
  }

  /**
   * Loads the specified id and a function to call when it is evaluated
   */
  @Override
  public <T> Lazy<T> load(final Class<T> clazz, final String id, Action1<T> onEval) {
    if (delegate.isLoaded(id)) {
      return new Lazy<T>(new Function0<T>() {
        @Override
        public T apply() {
          return delegate.load(clazz, id);
        }
      });
    }
    LazyLoadOperation<T> lazyLoadOperation = new LazyLoadOperation<T>(clazz, id, new LoadOperation(delegate,  delegate.new DisableAllCachingCallback(), id));
    return delegate.addLazyOperation(lazyLoadOperation, onEval);
  }

  @Override
  public <T> Lazy<T> load(Class<T> clazz, Number id, Action1<T> onEval) {
    String documentKey = delegate.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false);
    return load(clazz, documentKey, onEval);
  }

  @Override
  public <T> Lazy<T> load(Class<T> clazz, UUID id, Action1<T> onEval) {
    String documentKey = delegate.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false);
    return load(clazz, documentKey, onEval);
  }

  @Override
  public <T> Lazy<T[]> load(Class<T> clazz, Number... ids) {
    List<String> documentKeys = new ArrayList<>();
    for (Number id : ids) {
      documentKeys.add(delegate.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false));
    }
    return load(clazz, documentKeys, null);
  }

  public <T> Lazy<T[]> load(Class<T> clazz, UUID... ids) {
    List<String> documentKeys = new ArrayList<>();
    for (UUID id : ids) {
      documentKeys.add(delegate.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false));
    }
    return load(clazz, documentKeys, null);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <TResult> Lazy<TResult[]> load(Class<TResult> clazz, Action1<TResult[]> onEval, Number... ids) {
    List<String> documentKeys = new ArrayList<>();
    for (Number id : ids) {
      documentKeys.add(delegate.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false));
    }
    return delegate.lazyLoadInternal(clazz, documentKeys.toArray(new String[0]), new Tuple[0], onEval);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <TResult> Lazy<TResult[]> load(Class<TResult> clazz, Action1<TResult[]> onEval, UUID... ids) {
    List<String> documentKeys = new ArrayList<>();
    for (UUID id : ids) {
      documentKeys.add(delegate.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false));
    }
    return delegate.lazyLoadInternal(clazz, documentKeys.toArray(new String[0]), new Tuple[0], onEval);
  }


  /**
   * Begin a load while including the specified path
   */
  public ILazyLoaderWithInclude include(String path) {
    return new LazyMultiLoaderWithInclude(delegate).include(path);
  }

  public <T> Lazy<T> load(Class<T> clazz, Number id) {
    return load(clazz, id, (Action1<T>) null);
  }

  public <T> Lazy<T> load(Class<T> clazz, UUID id) {
    return load(clazz, id, (Action1<T>) null);
  }

  public <T> Lazy<T[]> loadStartingWith(Class<T> clazz, String keyPrefix) {
    return loadStartingWith(clazz, keyPrefix, null, 0, 25);
  }

  public <T> Lazy<T[]> loadStartingWith(Class<T> clazz, String keyPrefix, String matches) {
    return loadStartingWith(clazz, keyPrefix, matches, 0, 25);
  }

  public <T> Lazy<T[]> loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start) {
    return loadStartingWith(clazz, keyPrefix, matches, start, 25);
  }

  public <T> Lazy<T[]> loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start, int pageSize) {
    LazyStartsWithOperation<T> operation = new LazyStartsWithOperation<T>(clazz, keyPrefix, matches, start, pageSize, delegate);
    return delegate.addLazyOperation(operation, null);
  }


}
