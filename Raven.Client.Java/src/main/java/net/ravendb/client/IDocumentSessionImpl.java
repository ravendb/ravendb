package net.ravendb.client;

import net.ravendb.abstractions.basic.Lazy;
import net.ravendb.abstractions.basic.Tuple;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.client.document.DocumentConvention;
import net.ravendb.client.document.batches.IEagerSessionOperations;

/**
 * Interface for document session which holds the internal operations
 */
public interface IDocumentSessionImpl extends IDocumentSession, IEagerSessionOperations {

  public DocumentConvention getConventions();

  public <T> T[] loadInternal(Class<T> clazz, String[] ids);

  public <T> T[] loadInternal(Class<T> clazz, String[] ids, Tuple<String, Class<?>>[] includes);

  <T> Lazy<T[]> lazyLoadInternal(Class<T> clazz, String[] ids, Tuple<String, Class<?>>[] includes, Action1<T[]> onEval);
}
