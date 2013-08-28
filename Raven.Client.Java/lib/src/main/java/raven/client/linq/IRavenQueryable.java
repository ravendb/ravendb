package raven.client.linq;

import com.mysema.query.types.Path;

import raven.abstractions.basic.Reference;
import raven.abstractions.closure.Action1;
import raven.abstractions.closure.Function1;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.IDocumentQueryCustomization;
import raven.client.RavenQueryStatistics;
import raven.client.indexes.AbstractTransformerCreationTask;
import raven.client.spatial.SpatialCriteria;
import raven.client.spatial.SpatialCriteriaFactory;

/**
 * An implementation of IOrderedQueryable with Raven specific operation
 *
 * @param <T>
 */
public interface IRavenQueryable<T> extends IOrderedQueryable<T> {
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
  IRavenQueryable<T> customize(Action1<IDocumentQueryCustomization> action);

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

}
