package net.ravendb.client;

import java.util.List;
import java.util.Map;

import net.ravendb.abstractions.basic.Lazy;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.data.FacetResults;
import net.ravendb.abstractions.data.IndexQuery;
import net.ravendb.abstractions.data.QueryResult;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.client.spatial.SpatialCriteria;

import com.mysema.query.types.Path;


/**
 * A query against a Raven index
 *
 * @param <T>
 */
public interface IDocumentQuery<T> extends IDocumentQueryBase<T, IDocumentQuery<T>>, Iterable<T> {
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

  public IDocumentQuery<T> spatial(Path<?> path, SpatialCriteria criteria);

  public IDocumentQuery<T> spatial(String name, SpatialCriteria criteria);

  public boolean isDistinct();

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
   * Query the facets results for this query using the specified facet document with the given start and pageSize
   * @param facetSetupDoc
   * @param start
   * @param pageSize
   * @return
   */
  public Lazy<FacetResults> toFacetsLazy(String facetSetupDoc);

  /**
   * Query the facets results for this query using the specified facet document with the given start and pageSize
   * @param facetSetupDoc
   * @param start
   * @param pageSize
   * @return
   */
  public Lazy<FacetResults> toFacetsLazy(String facetSetupDoc, int start);

  /**
   * Query the facets results for this query using the specified facet document with the given start and pageSize
   * @param facetSetupDoc
   * @param start
   * @param pageSize
   * @return
   */
  public Lazy<FacetResults> toFacetsLazy(String facetSetupDoc, int start, Integer pageSize);

  /**
   * Query the facets results for this query using the specified list of facets with the given start and pageSize
   * @param facets
   * @param start
   * @param pageSize
   * @return
   */
  public Lazy<FacetResults> toFacetsLazy(List<Facet> facets);

  /**
   * Query the facets results for this query using the specified list of facets with the given start and pageSize
   * @param facets
   * @param start
   * @param pageSize
   * @return
   */
  public Lazy<FacetResults> toFacetsLazy(List<Facet> facets, int start);

  /**
   * Query the facets results for this query using the specified list of facets with the given start and pageSize
   * @param facets
   * @param start
   * @param pageSize
   * @return
   */
  public Lazy<FacetResults> toFacetsLazy(List<Facet> facets, int start, Integer pageSize);

  /**
   * Query the facets results for this query using the specified facet document with the given start and pageSize
   * @param facetSetupDoc
   * @param start
   * @param pageSize
   * @return
   */
  public FacetResults toFacets(String facetSetupDoc);

  /**
   * Query the facets results for this query using the specified facet document with the given start and pageSize
   * @param facetSetupDoc
   * @param start
   * @param pageSize
   * @return
   */
  public FacetResults toFacets(String facetSetupDoc, int start);

  /**
   * Query the facets results for this query using the specified facet document with the given start and pageSize
   * @param facetSetupDoc
   * @param start
   * @param pageSize
   * @return
   */
  public FacetResults toFacets(String facetSetupDoc, int start, Integer pageSize);

  /**
   * Query the facets results for this query using the specified list of facets with the given start and pageSize
   * @param facets
   * @param start
   * @param pageSize
   * @return
   */
  public FacetResults toFacets(List<Facet> facets);

  /**
   * Query the facets results for this query using the specified list of facets with the given start and pageSize
   * @param facets
   * @param start
   * @param pageSize
   * @return
   */
  public FacetResults toFacets(List<Facet> facets, int start);

  /**
   * Query the facets results for this query using the specified list of facets with the given start and pageSize
   * @param facets
   * @param start
   * @param pageSize
   * @return
   */
  public FacetResults toFacets(List<Facet> facets, int start, Integer pageSize);

  /**
   * Returns first result
   * @return
   */
  public T first();

  /**
   * Returns first result
   * @return
   */
  public T firstOrDefault();

  /**
   * Materialize query, executes request and returns with results
   * @return
   */
  public List<T> toList();

  /**
   * Returns single result
   * @return
   */
  public T single();

  /**
   * Returns single result
   * @return
   */
  public T singleOrDefault();

  /**
   * Returns if any entry matches query
   * @return
   */
  public boolean any();

}
