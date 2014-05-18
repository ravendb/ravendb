package net.ravendb.client.document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import net.ravendb.abstractions.basic.Tuple;
import net.ravendb.abstractions.extensions.ExpressionExtensions;
import net.ravendb.client.IDocumentSessionImpl;

import com.google.common.base.Defaults;
import com.mysema.query.types.Path;


/**
 * Fluent implementation for specifying include paths
 * for loading documents
 *
 * @param <T>
 */
public class MultiLoaderWithInclude implements ILoaderWithInclude {
  private final IDocumentSessionImpl session;
  private final List<Tuple<String, Class<?>>> includes = new ArrayList<>();

  public MultiLoaderWithInclude(IDocumentSessionImpl session) {
    this.session = session;
  }

  @Override
  public ILoaderWithInclude include(Class<?> targetClass, Path<?> path) {
    Class< ? > type = path.getType();
    String fullId = session.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().find(-1, targetClass, false);
    String id = ExpressionExtensions.toPropertyPath(path);
    if (!String.class.equals(type)) {
      String idPrefix = fullId.replace("-1", "");
      id += "(" + idPrefix + ")";
    }
    return include(id, targetClass);
  }

  /**
   * Includes the specified path.
   */
  @Override
  public ILoaderWithInclude include(String path) {
    return include(path, Object.class);
  }

  public ILoaderWithInclude include(String path, Class<?> type) {
    includes.add(new Tuple<String, Class<?>>(path, type));
    return this;
  }

  /**
   * Includes the specified path
   */
  @Override
  public ILoaderWithInclude include(Path<?> path) {
    return include(ExpressionExtensions.toPropertyPath(path));
  }

  @SuppressWarnings("cast")
  @Override
  public <TResult> TResult[] load(Class<TResult> clazz, String... ids) {
    return session.loadInternal(clazz, ids, (Tuple<String, Class<?>>[])includes.toArray(new Tuple[0]));
  }

  @SuppressWarnings("cast")
  @Override
  public <TResult> TResult[] load(Class<TResult> clazz, Collection<String> ids) {
    return session.loadInternal(clazz, ids.toArray(new String[0]), (Tuple<String, Class<?>>[])includes.toArray(new Tuple[0]));
  }

  @SuppressWarnings("cast")
  @Override
  public <TResult> TResult load(Class<TResult> clazz, String id) {
    TResult[] results = session.loadInternal(clazz, new String[] { id }, (Tuple<String, Class<?>>[])includes.toArray(new Tuple[0]));
    return results.length > 0 ? results[0] : Defaults.defaultValue(clazz);
  }

  @Override
  public <TResult> TResult load(Class<TResult> clazz, Number id) {
    String documentKey = session.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().find(id, clazz, false);
    return load(clazz, documentKey);
  }

  @Override
  public <TResult> TResult load(Class<TResult> clazz, UUID id) {
    String documentKey = session.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().find(id, clazz, false);
    return load(clazz, documentKey);
  }

  @Override
  public <TResult> TResult[] load(Class<TResult> clazz, UUID... ids) {
    List<String> documentIds = new ArrayList<>();
    for (UUID id: ids) {
      documentIds.add(session.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().find(id, clazz, false));
    }
    return load(clazz, documentIds);
  }

  @Override
  public <TResult> TResult[] load(Class<TResult> clazz, Number... ids) {
    List<String> documentIds = new ArrayList<>();
    for (Number id: ids) {
      documentIds.add(session.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().find(id, clazz, false));
    }
    return load(clazz, documentIds);
  }
}
