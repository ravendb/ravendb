package net.ravendb.client;

import java.util.List;

import net.ravendb.abstractions.basic.Lazy;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.data.FacetResults;
import net.ravendb.abstractions.data.SuggestionQuery;
import net.ravendb.abstractions.data.SuggestionQueryResult;
import net.ravendb.client.linq.DynamicAggregationQuery;
import net.ravendb.client.linq.IRavenQueryable;


import com.mysema.query.types.Path;

public interface LinqExtensionsQueryable<T> {

  public IRavenQueryable<T> include(Path<?> path);

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

  public IRavenQueryable<T> intersect();

  public SuggestionQueryResult suggest();

  public SuggestionQueryResult suggest(SuggestionQuery query);

  public Lazy<SuggestionQueryResult> suggestLazy();

  public Lazy<SuggestionQueryResult> suggestLazy(SuggestionQuery query);

  public Lazy<List<T>> lazily();

  public Lazy<List<T>> lazily(Action1<List<T>> onEval);

  /**
   *  Perform a search for documents which fields that match the searchTerms.
   *  If there is more than a single term, each of them will be checked independently.
   * @param fieldSelector
   * @param searchTerms
   * @return
   */
  public IRavenQueryable<T> search(Path<?> fieldSelector, String searchTerms);

  public IRavenQueryable<T> search(Path<?> fieldSelector, String searchTerms, double boost);

  public IRavenQueryable<T> search(Path<?> fieldSelector, String searchTerms, double boost, SearchOptionsSet searchOptions);

  public IRavenQueryable<T> search(Path<?> fieldSelector, String searchTerms, double boost, SearchOptionsSet searchOptions, EscapeQueryOptions escapeQueryOptions);

  public IRavenQueryable<T> orderByScore();

  public Lazy<Integer> countLazily();

}
