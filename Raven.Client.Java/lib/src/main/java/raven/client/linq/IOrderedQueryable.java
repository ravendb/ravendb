package raven.client.linq;

import java.util.List;

import raven.abstractions.basic.Lazy;
import raven.abstractions.data.Facet;
import raven.abstractions.data.FacetResults;

import com.mysema.query.types.Expression;
import com.mysema.query.types.OrderSpecifier;
import com.mysema.query.types.Path;
import com.mysema.query.types.Predicate;

//TODO: insert linq expressions  + RavenQueryableExtensions + LinqExntesions
public interface IOrderedQueryable<T> {
  /**
   * Filters a sequence of values based on a predicate.
   * @param predicate
   * @return
   */
  public IRavenQueryable<T> where(Predicate predicate);

  /**
   * Changes order of result elements
   * @param asc
   * @return
   */
  public IRavenQueryable<T> orderBy(OrderSpecifier<?>... asc);

  //TODO: finish me

  public List<T> toList();

  public T single();

  public Lazy<List<T>> lazily();


  //IQueryable

  public Class<?> getElementType();

  public Expression<?> getExpression();

  public IQueryProvider getProvider();

  /**
   * Project using a different type
   * @param clazz
   * @return
   */
  public <TResult> IRavenQueryable<TResult> as(Class<TResult> clazz);

  // LinqExtensions

  /**
   * Query the facets results for this query using aggregation
   */
  public DynamicAggregationQuery<T> aggregateBy(String path);

  /**
   * Query the facets results for this query using aggregation with a specific display name
   * @param path
   * @param displayName
   * @return
   */
  public DynamicAggregationQuery<T> aggregateBy(String path, String displayName);

  /**
   * Query the facets results for this query using aggregation
   * @param path
   * @return
   */
  public DynamicAggregationQuery<T> aggregateBy(Path<?> path);

  /**
   * Query the facets results for this query using aggregation with a specific display name
   * @param path
   * @param displayName
   * @return
   */
  public DynamicAggregationQuery<T> aggregateBy(Path<?> path, String displayName);

  /**
   * Query the facets results for this query using the specified list of facets
   * @param facets
   * @param start
   * @param pageSize
   * @return
   */
  public Lazy<FacetResults> toFacetsLazy(List<Facet> facets);

  /**
   * Query the facets results for this query using the specified list of facets with the given start
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
   * Query the facets results for this query using the specified facet document
   * @param facetSetupDoc
   * @param start
   * @param pageSize
   * @return
   */
  public Lazy<FacetResults> toFacetsLazy(String facetSetupDoc);

  /**
   * Query the facets results for this query using the specified facet document with the given start
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
   * Query the facets results for this query using the specified list of facets
   * @param facets
   * @param start
   * @param pageSize
   * @return
   */
  public FacetResults toFacets(List<Facet> facets);

  /**
   * Query the facets results for this query using the specified list of facets with the given start
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
   * Query the facets results for this query using the specified facet document
   * @param facetSetupDoc
   * @param start
   * @param pageSize
   * @return
   */
  public FacetResults toFacets(String facetSetupDoc);

  /**
   * Query the facets results for this query using the specified facet document with the given start
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

}
