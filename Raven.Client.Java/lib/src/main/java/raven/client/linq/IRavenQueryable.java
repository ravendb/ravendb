package raven.client.linq;

import raven.abstractions.basic.Reference;
import raven.abstractions.closure.Function1;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.LinqExtensionsQueryable;
import raven.client.RavenQueryStatistics;
import raven.client.document.DocumentQueryCustomizationFactory;
import raven.client.indexes.AbstractTransformerCreationTask;
import raven.client.spatial.SpatialCriteria;
import raven.client.spatial.SpatialCriteriaFactory;

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
  <S> IRavenQueryable<S> transformWith(Class<? extends AbstractTransformerCreationTask> transformerClazz, Class<S> resultClass);

  /**
   * Inputs a key and value to the query (accessible by the transformer)
   * @param name
   * @param value
   * @return
   */
  IRavenQueryable<T> addQueryInput(String name, RavenJToken value);

  IRavenQueryable<T> spatial(Path<?> path, Function1<SpatialCriteriaFactory, SpatialCriteria> clause);

  /**
   * Returns distinct results
   * @return
   */
  IRavenQueryable<T> distinct();

}
