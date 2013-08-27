package raven.client;

import java.util.List;
import java.util.Map;

import com.mysema.query.types.Path;

import raven.abstractions.basic.Lazy;
import raven.abstractions.closure.Action1;
import raven.abstractions.closure.Function1;
import raven.abstractions.data.Facet;
import raven.abstractions.data.FacetResults;
import raven.abstractions.data.IndexQuery;
import raven.abstractions.data.QueryResult;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.spatial.SpatialCriteria;
import raven.client.spatial.SpatialCriteriaFactory;

/**
 * A query against a Raven index
 *
 * @param <T>
 */
public interface IDocumentQuery<T> extends IDocumentQueryBase<T, IDocumentQuery<T>> {
  /**
   * Selects the specified fields directly from the index
   * @param projectionClass The class of the projection
   * @param fields The fields.
   * @return
   */
  public <TProjection> IDocumentQuery<TProjection> selectFields(Class<TProjection> projectionClass, String... fields);

  /**
   * Selects the specified fields directly from the index
   * @param projectionClass The class of the projection
   * @param fields
   * @param projections
   * @return
   */
  public <TProjection> IDocumentQuery<TProjection> selectFields(Class<TProjection> projectionClass, String[] fields, String[] projections);

  /**
   * Selects the projection fields directly from the index
   * @param projectionClass The class of the projection
   * @return
   */
  public <TProjection> IDocumentQuery<TProjection> selectFields(Class<TProjection> projectionClass);

  /**
   * Sets user defined inputs to the query
   * @param queryInputs
   */
  public void setQueryInputs(Map<String, RavenJToken> queryInputs);

  /**
   * Gets the query result
   * Execute the query the first time that this is called.
   * @return The query result.
   */
  public QueryResult getQueryResult();

  /**
   * Register the query as a lazy query in the session and return a lazy
   * instance that will evaluate the query only when needed
   * @return
   */
  public Lazy<List<T>> lazily();

  /**
   * Register the query as a lazy query in the session and return a lazy
   * instance that will evaluate the query only when needed.
   * Also provide a function to execute when the value is evaluated
   * @param onEval
   * @return
   */
  public Lazy<List<T>> lazily(Action1<List<T>> onEval);

  /**
   * Create the index query object for this query
   * @return
   */
  public IndexQuery getIndexQuery();

  public IDocumentQuery<T> spatial(Path<?> path, Function1<SpatialCriteriaFactory, SpatialCriteria> clause);

  public IDocumentQuery<T> spatial(String name, Function1<SpatialCriteriaFactory, SpatialCriteria> clause);

  /**
   * Get the facets as per the specified doc with the given start and pageSize
   * @param facetSetupDoc
   * @param facetStart
   * @param facetPageSize
   * @return
   */
  public FacetResults getFacets(String facetSetupDoc, int facetStart, Integer facetPageSize);

  /**
   * Get the facets as per the specified facets with the given start and pageSize
   * @param facets
   * @param facetStart
   * @param facetPageSize
   * @return
   */
  public FacetResults getFacets(List<Facet> facets, int facetStart, Integer facetPageSize);

  /**
   * Returns first result
   * @return
   */
  public T first();

  /**
   * Materialize query, executes request and returns with results
   * @return
   */
  public List<T> toList();

}
