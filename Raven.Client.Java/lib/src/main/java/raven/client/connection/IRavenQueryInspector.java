package raven.client.connection;

import java.util.List;
import java.util.Map;

import raven.abstractions.data.Facet;
import raven.abstractions.data.FacetResults;
import raven.abstractions.data.IndexQuery;
import raven.client.IDocumentQuery;
import raven.client.document.InMemoryDocumentSessionOperations;

/**
 * Provide access to the underlying {@link IDocumentQuery}
 */
public interface IRavenQueryInspector {

  /**
   *  Get the name of the index being queried
   * @return
   */
  public String getIndexQueried();


  /**
   * Grant access to the database commands
   * @return
   */
  public IDatabaseCommands getDatabaseCommands();

  /**
   * The query session
   * @return
   */
  public InMemoryDocumentSessionOperations getSession();

  /**
   * The last term that we asked the query to use equals on
   * @return
   */
  public Map.Entry<String, String> getLastEqualityTerm();

  /**
   * Get the index query for this query
   * @return
   */
  public IndexQuery getIndexQuery();

  /**
   * Get the facets as per the specified facet document with the given start and pageSize
   * @param facetSetupDoc
   * @param start
   * @param pageSize
   * @return
   */
  public FacetResults getFacets(String facetSetupDoc, int start, Integer pageSize);

  /**
   *  Get the facet results as per the specified facets with the given start and pageSize
   * @param facets
   * @param start
   * @param pageSize
   * @return
   */
  public FacetResults getFacets(List<Facet> facets, int start, Integer pageSize);

}
