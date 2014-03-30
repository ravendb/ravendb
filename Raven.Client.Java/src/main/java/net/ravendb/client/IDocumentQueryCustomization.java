package net.ravendb.client;

import java.util.Date;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.IndexQuery;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialRelation;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialUnits;
import net.ravendb.client.spatial.SpatialCriteria;

import com.mysema.query.types.Path;


/**
 * Customize the document query
 */
public interface IDocumentQueryCustomization {
  /**
   * Instructs the query to wait for non stale results as of the last write made by any session belonging to the
   * current document store.
   * This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
   * However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results.
   * @return
   */
  public IDocumentQueryCustomization waitForNonStaleResultsAsOfLastWrite();

  /**
   * Instructs the query to wait for non stale results as of the last write made by any session belonging to the
   * current document store.
   * This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
   * However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results.
   * @param waitTimeout
   * @return
   */
  public IDocumentQueryCustomization waitForNonStaleResultsAsOfLastWrite(long waitTimeout);

  /**
   * Instructs the query to wait for non stale results as of now.
   * @return
   */
  public IDocumentQueryCustomization waitForNonStaleResultsAsOfNow();

  /**
   * Instructs the query to wait for non stale results as of now for the specified timeout.
   * @param waitTimeout timeout in milis
   * @return
   */
  public IDocumentQueryCustomization waitForNonStaleResultsAsOfNow(long waitTimeout);

  /**
   * Instructs the query to wait for non stale results as of the cutoff date.
   * @param cutOff
   * @return
   */
  public IDocumentQueryCustomization waitForNonStaleResultsAsOf(Date cutOff);

  /**
   * Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
   * @param cutOff
   * @param waitTimeout timeout in milis
   * @return
   */
  public IDocumentQueryCustomization waitForNonStaleResultsAsOf(Date cutOff, long waitTimeout);

  /**
   * Instructs the query to wait for non stale results as of the cutoff etag.
   * @param cutOffEtag
   * @return
   */
  public IDocumentQueryCustomization waitForNonStaleResultsAsOf(Etag cutOffEtag);

  /**
   * Instructs the query to wait for non stale results as of the cutoff etag for the specified timeout.
   * @param cutOffEtag
   * @param waitTimeout
   * @return
   */
  public IDocumentQueryCustomization waitForNonStaleResultsAsOf(Etag cutOffEtag, long waitTimeout);

  /**
   * EXPERT ONLY: Instructs the query to wait for non stale results.
   * This shouldn't be used outside of unit tests unless you are well aware of the implications
   * @return
   */
  public IDocumentQueryCustomization waitForNonStaleResults();

  /**
   * Includes the specified path in the query, loading the document specified in that path
   * @param path
   * @return
   */
  public IDocumentQueryCustomization include(Path<?> path);

  /**
   * Includes the specified path in the query, loading the document specified in that path
   * @param path
   * @return
   */
  public IDocumentQueryCustomization include(String path);

  /**
   * Includes the specified path in the query, loading the document specified in that path
   * @param path
   * @return
   */
  public IDocumentQueryCustomization include(Class<?> targetClass, Path<?> path);

  /**
   * EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
   * This shouldn't be used outside of unit tests unless you are well aware of the implications
   * @param waitTimeout
   * @return
   */
  public IDocumentQueryCustomization waitForNonStaleResults(long waitTimeout);

  /**
   * Filter matches to be inside the specified radius
   * @param radius
   * @param latitude
   * @param longitude
   * @return
   */
  public IDocumentQueryCustomization withinRadiusOf(double radius, double latitude, double longitude);

  /**
   * Filter matches to be inside the specified radius
   * @param fieldName
   * @param radius
   * @param latitude
   * @param longitude
   * @return
   */
  public IDocumentQueryCustomization withinRadiusOf(String fieldName, double radius, double latitude, double longitude);

  /**
   * Filter matches to be inside the specified radius
   * @param radius
   * @param latitude
   * @param longitude
   * @param radiusUnits
   * @return
   */
  public IDocumentQueryCustomization withinRadiusOf(double radius, double latitude, double longitude, SpatialUnits radiusUnits);

  /**
   * Filter matches to be inside the specified radius
   * @param fieldName
   * @param radius
   * @param latitude
   * @param longitude
   * @param radiusUnits
   * @return
   */
  public IDocumentQueryCustomization withinRadiusOf(String fieldName, double radius, double latitude, double longitude, SpatialUnits radiusUnits);

  /**
   * Filter matches based on a given shape - only documents with the shape defined in fieldName that
   * have a relation rel with the given shapeWKT will be returned
   * @param fieldName The name of the field containing the shape to use for filtering
   * @param shapeWKT The query shape
   * @param rel Spatial relation to check
   * @return
   */
  public IDocumentQueryCustomization relatesToShape(String fieldName, String shapeWKT, SpatialRelation rel);

  public IDocumentQueryCustomization spatial(String fieldName, SpatialCriteria criteria);

  /**
   * When using spatial queries, instruct the query to sort by the distance from the origin point
   * @return
   */
  public IDocumentQueryCustomization sortByDistance();

  /**
   * Order the search results randomly
   * @return
   */
  public IDocumentQueryCustomization randomOrdering();

  /**
   * Order the search results randomly using the specified seed
   * this is useful if you want to have repeatable random queries
   * @param seed
   * @return
   */
  public IDocumentQueryCustomization randomOrdering(String seed);

  /**
   * Allow you to modify the index query before it is executed
   * @param action
   * @return
   */
  public IDocumentQueryCustomization beforeQueryExecution(Action1<IndexQuery> action);

  /**
   * Adds matches highlighting for the specified field.
   *
   * The specified field should be analysed and stored for highlighter to work.
   *  For each match it creates a fragment that contains matched text surrounded by highlighter tags.
   * @param fieldName The field name to highlight.
   * @param fragmentLength The fragment length.
   * @param fragmentCount The maximum number of fragments for the field.
   * @param fragmentsField The field in query results item to put highlightings into.
   * @return
   */
  public IDocumentQueryCustomization highlight(String fieldName, int fragmentLength, int fragmentCount, String fragmentsField);

  /**
   * Adds matches highlighting for the specified field.
   *
   * The specified field should be analysed and stored for highlighter to work.
   * For each match it creates a fragment that contains matched text surrounded by highlighter tags.
   * @param fieldName The field name to highlight.
   * @param fragmentLength The fragment length.
   * @param fragmentCount The maximum number of fragments for the field.
   * @param highlightings Field highlightings for all results.
   * @return
   */
  public IDocumentQueryCustomization highlight(String fieldName, int fragmentLength, int fragmentCount, Reference<FieldHighlightings> highlightings);

  /**
   * Sets the tags to highlight matches with.
   * @param preTag Prefix tag.
   * @param postTag Postfix tag.
   * @return
   */
  public IDocumentQueryCustomization setHighlighterTags(String preTag, String postTag);

  /**
   * Sets the tags to highlight matches with.
   * @param preTags Prefix tags.
   * @param postTags Postfix tags.
   * @return
   */
  public IDocumentQueryCustomization setHighlighterTags(String[] preTags, String[] postTags);

  /**
   * Disables tracking for queried entities by Raven's Unit of Work.
   * Usage of this option will prevent holding query results in memory.
   * @return
   */
  public IDocumentQueryCustomization noTracking();

  /**
   * Disables caching for query results.
   * @return
   */
  public IDocumentQueryCustomization noCaching();
}
