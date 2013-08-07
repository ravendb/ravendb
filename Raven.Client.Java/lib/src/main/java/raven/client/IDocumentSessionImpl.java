package raven.client;

import raven.abstractions.basic.Lazy;
import raven.abstractions.basic.Tuple;
import raven.abstractions.closure.Action1;
import raven.client.document.DocumentConvention;
import raven.client.document.batches.IEagerSessionOperations;
import raven.client.document.batches.ILazySessionOperations;

/**
 * Interface for document session which holds the internal operations
 */
public interface IDocumentSessionImpl extends IDocumentSession, ILazySessionOperations, IEagerSessionOperations {

  public DocumentConvention getConventions();

  public <T> T[] loadInternal(Class<T> clazz, String[] ids);

  public <T> T[] loadInternal(Class<T> clazz, String[] ids, Tuple<String, Class<?>>[] includes);

  <T> Lazy<T[]> lazyLoadInternal(Class<T> clazz, String[] ids, Tuple<String, Class<?>>[] includes, Action1<T[]> onEval);
}
