package net.ravendb.client.linq;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.client.LinqExtensionsQueryable;
import net.ravendb.client.RavenQueryStatistics;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.indexes.AbstractTransformerCreationTask;
import net.ravendb.client.spatial.SpatialCriteria;

import com.mysema.query.types.Path;

/**
 * An implementation of IOrderedQueryable with Raven specific operation
 *
 * @param <T>
 */
public interface IRavenQueryable<T> extends IOrderedQueryable<T>, LinqExtensionsQueryable<T> {
  /**
   * Provide statistics about the query, such as total count of matching records
   * @param stats
   * @return
   */
  IRavenQueryable<T> statistics(Reference<RavenQueryStatistics> stats);

  /**
   * Customizes the query using the specified action
   * @param action
   * @return
   */
  IRavenQueryable<T> customize(DocumentQueryCustomizationFactory customizationFactory);

  /**
   * Specifies a result transformer to use on the results
   * @param transformerClazz
   * @param resultClass
   * @return
   */
  <S> IRavenQueryable<S> transformWith(String transformerName, Class<S> resultClass);


  /**
   * Specifies a result transformer to use on the results
   * @param transformerClazz
   * @param resultClass
   * @return
   */
  <S> IRavenQueryable<S> transformWith(Class<? extends AbstractTransformerCreationTask> transformerClazz, Class<S> resultClass);

  /**
   * Inputs a key and value to the query (accessible by the transformer)
   * @param name
   * @param value
   * @return
   */
  IRavenQueryable<T> addQueryInput(String name, RavenJToken value);

  /**
   * Inputs a key and value to the query (accessible by the transformer)
   * @param name
   * @param value
   * @return
   */
  IRavenQueryable<T> addQueryInput(String name, Object value);

  IRavenQueryable<T> spatial(Path<?> path, SpatialCriteria criteria);

  /**
   * Returns distinct results
   * @return
   */
  IRavenQueryable<T> distinct();

}
