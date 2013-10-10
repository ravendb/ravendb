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
import raven.client.document.batches.LazyMultiLoadOperation;
import raven.client.document.batches.LazyMultiLoaderWithInclude;
import raven.client.document.batches.LazyStartsWithOperation;
import raven.client.document.batches.LazyTransformerLoadOperation;
import raven.client.document.sessionoperations.LoadOperation;
import raven.client.document.sessionoperations.LoadTransformerOperation;
import raven.client.document.sessionoperations.MultiLoadOperation;
import raven.client.indexes.AbstractTransformerCreationTask;

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
      return new Lazy<>(new Function0<T>() {
        @Override
        public T apply() {
          return delegate.load(clazz, id);
        }
      });
    }
    LazyLoadOperation<T> lazyLoadOperation = new LazyLoadOperation<>(clazz, id, new LoadOperation(delegate,  delegate.new DisableAllCachingCallback(), id));
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

  @Override
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



  @Override
  public <TResult, TTransformer extends AbstractTransformerCreationTask> Lazy<TResult> load(
    Class<TTransformer> tranformerClass, Class<TResult> clazz, String id) {
    try {
      String transformer = tranformerClass.newInstance().getTransformerName();
      LoadTransformerOperation loadOperation = new LoadTransformerOperation(delegate, transformer, 1);
      LazyTransformerLoadOperation<TResult> lazyLoadOperation = new LazyTransformerLoadOperation<>(clazz, id, transformer, loadOperation, true);
      return delegate.addLazyOperation(lazyLoadOperation, null);
    } catch (IllegalAccessException | InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <TResult, TTransformer extends AbstractTransformerCreationTask> Lazy<TResult[]> load(
    Class<TTransformer> tranformerClass, Class<TResult> clazz, String... ids) {
    try {
      String transformer = tranformerClass.newInstance().getTransformerName();
      MultiLoadOperation multiLoadOperation = new MultiLoadOperation(delegate, new Function0<AutoCloseable>() {
        @Override
        public AutoCloseable apply() {
          return delegate.getDatabaseCommands().disableAllCaching();
        }
      }, ids, null);
      LazyMultiLoadOperation<TResult> lazyLoadOperation = new LazyMultiLoadOperation<>(clazz,  multiLoadOperation, ids, null, transformer);
      return delegate.addLazyOperation(lazyLoadOperation, null);
    } catch (IllegalAccessException | InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Begin a load while including the specified path
   */
  @Override
  public ILazyLoaderWithInclude include(String path) {
    return new LazyMultiLoaderWithInclude(delegate).include(path);
  }

  @Override
  public <T> Lazy<T> load(Class<T> clazz, Number id) {
    return load(clazz, id, (Action1<T>) null);
  }

  @Override
  public <T> Lazy<T> load(Class<T> clazz, UUID id) {
    return load(clazz, id, (Action1<T>) null);
  }

  @Override
  public <T> Lazy<T[]> loadStartingWith(Class<T> clazz, String keyPrefix) {
    return loadStartingWith(clazz, keyPrefix, null, 0, 25);
  }

  @Override
  public <T> Lazy<T[]> loadStartingWith(Class<T> clazz, String keyPrefix, String matches) {
    return loadStartingWith(clazz, keyPrefix, matches, 0, 25);
  }

  @Override
  public <T> Lazy<T[]> loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start) {
    return loadStartingWith(clazz, keyPrefix, matches, start, 25);
  }

  @Override
  public <T> Lazy<T[]> loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start, int pageSize) {
    return loadStartingWith(clazz, keyPrefix, matches, start, 25, null);
  }

  @Override
  public <TResult> Lazy<TResult[]> loadStartingWith(Class<TResult> clazz, String keyPrefix, String matches, int start,
    int pageSize, String exclude) {
    LazyStartsWithOperation<TResult> operation = new LazyStartsWithOperation<>(clazz, keyPrefix, matches, exclude, start, pageSize, delegate);
    return delegate.addLazyOperation(operation, null);
  }




}
