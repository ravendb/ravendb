package raven.client;

import com.mysema.commons.lang.Pair;

import raven.client.document.DocumentConvention;
import raven.client.document.batches.IEagerSessionOperations;
import raven.client.document.batches.ILazySessionOperations;

/**
 * Interface for document session which holds the internal operations
 */
public interface IDocumentSessionImpl extends IDocumentSession, ILazySessionOperations, IEagerSessionOperations {

  public DocumentConvention getConventions();

  public <T> T[] loadInternal(Class<T> clazz, String[] ids);

  public <T> T[] loadInternal(Class<T> clazz, String[] ids, Pair<String, Class<?>>[] includes);

  //TODO: Lazy<T[]> LazyLoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, Action<T[]> onEval);
}
