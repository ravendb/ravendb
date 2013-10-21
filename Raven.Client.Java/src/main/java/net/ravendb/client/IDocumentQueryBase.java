package net.ravendb.client;

import java.util.Collection;
import java.util.Date;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.IndexQuery;
import net.ravendb.abstractions.data.QueryOperator;
import net.ravendb.abstractions.data.QueryResult;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialRelation;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialUnits;
import net.ravendb.client.document.DocumentConvention;

import com.mysema.query.types.Path;
import com.mysema.query.types.path.ListPath;


/**
 * A query against a Raven index
 */
public interface IDocumentQueryBase<T, TSelf extends IDocumentQueryBase<T, TSelf>> {

  /**
   * Gets the document convention from the query session
   * @return
   */
  public DocumentConvention getDocumentConvention();

  /**
   *  Negate the next operation
   * @return
   */
  public TSelf not();

  /**
   * Negate the next operation
   */
  public void negateNext();

  /**
   * Includes the specified path in the query, loading the document specified in that path
   */
  public TSelf include(String path);

  /**
   * Includes the specified path in the query, loading the document specified in that path
   * @param path The path.
   * @return
   */
  public TSelf include(Path<?> path);

  /**
   * Takes the specified count.
   * @param count The count.
   * @return
   */
  public TSelf take (int count);

  /**
   * Skips the specified count.
   * @param count The count.
   * @return
   */
  public TSelf skip(int count);

  /**
   * Filter the results from the index using the specified where clause.
   * @param whereClause The where clause.
   * @return
   */
  public TSelf where(String whereClause);

  /**
   * Matches exact value
   * Defaults to NotAnalyzed
   * @param fieldName
   * @param value
   * @return
   */
  public TSelf whereEquals(String fieldName, Object value);

  /**
   * Matches exact value
   *
   * Defaults to NotAnalyzed
   * @param propertySelector
   * @param value
   * @return
   */
  public <TValue> TSelf whereEquals(Path<? super TValue> propertySelector, TValue value);

  /**
   * Matches exact value
   *
   * Default to allow wildcard only if analyzed
   * @param fieldName
   * @param value
   * @param isAnalyzed
   * @return
   */
  public TSelf whereEquals(String fieldName, Object value, boolean isAnalyzed);

  /**
   * Matches exact value
   *
   * Defaults to allow wildcards only if analyzed
   * @param propertySelector
   * @param value
   * @param isAnalyzed
   * @return
   */
  public <TValue> TSelf whereEquals(Path<? super TValue> propertySelector, TValue value, boolean isAnalyzed);

  /**
   * Matches exact value
   * @param whereParams
   * @return
   */
  public TSelf whereEquals (WhereParams whereParams);

  /**
   * Check that the field has one of the specified value
   * @param fieldName
   * @param values
   * @return
   */
  public TSelf whereIn(String fieldName, Collection<?> values);

  /**
   * Check that the field has one of the specified value
   * @param propertySelector
   * @param values
   * @return
   */
  public <TValue> TSelf whereIn(Path<? super TValue> propertySelector, Collection<TValue> values);

  /**
   * Matches fields which starts with the specified value.
   * @param fieldName Name of the field.
   * @param value The value.
   * @return
   */
  public TSelf whereStartsWith(String fieldName, Object value);

  /**
   * Matches fields which starts with the specified value.
   * @param propertySelector Property selector for the field.
   * @param value The value.
   * @return
   */
  public <TValue> TSelf whereStartsWith(Path<? super TValue> propertySelector, TValue value);

  /**
   * Matches fields which ends with the specified value.
   * @param fieldName Name of the field.
   * @param value The value.
   * @return
   */
  public TSelf whereEndsWith (String fieldName, Object value);

  /**
   * Matches fields which ends with the specified value.
   * @param propertySelector Property selector for the field.
   * @param value The value.
   * @return
   */
  public <TValue> TSelf whereEndsWith(Path<? super TValue> propertySelector, TValue value);

  /**
   * Matches fields where the value is between the specified start and end, exclusive
   * @param fieldName Name of the field.
   * @param start The start.
   * @param end The end.
   * @return
   */
  public TSelf whereBetween (String fieldName, Object start, Object end);

  /**
   * Matches fields where the value is between the specified start and end, exclusive
   * @param propertySelector Property selector for the field.
   * @param start The start.
   * @param end The end.
   * @return
   */
  public <TValue> TSelf whereBetween(Path<? super TValue> propertySelector, TValue start, TValue end);

  /**
   * Matches fields where the value is between the specified start and end, inclusive
   * @param fieldName Name of the field.
   * @param start The start.
   * @param end The end.
   * @return
   */
  public TSelf whereBetweenOrEqual (String fieldName, Object start, Object end);

  /**
   * Matches fields where the value is between the specified start and end, inclusive
   * @param propertySelector Property selector for the field.
   * @param start The start.
   * @param end The end.
   * @return
   */
  public <TValue> TSelf whereBetweenOrEqual(Path<? super TValue> propertySelector, TValue start, TValue end);

  /**
   * Matches fields where the value is greater than the specified value
   * @param fieldName Name of the field.
   * @param value The value.
   * @return
   */
  public TSelf whereGreaterThan (String fieldName, Object value);

  /**
   * Matches fields where the value is greater than the specified value
   * @param propertySelector Property selector for the field.
   * @param value The value.
   * @return
   */
  public <TValue> TSelf whereGreaterThan(Path<? super TValue> propertySelector, TValue value);

  /**
   * Matches fields where the value is greater than or equal to the specified value
   * @param fieldName Name of the field.
   * @param value The value.
   * @return
   */
  public TSelf whereGreaterThanOrEqual (String fieldName, Object value);

  /**
   * Matches fields where the value is greater than or equal to the specified value
   * @param propertySelector Property selector for the field.
   * @param value The value.
   * @return
   */
  public <TValue> TSelf whereGreaterThanOrEqual(Path<? super TValue> propertySelector, TValue value);

  /**
   * Matches fields where the value is less than the specified value
   * @param fieldName Name of the field.
   * @param value The value.
   * @return
   */
  public TSelf whereLessThan (String fieldName, Object value);

  /**
   * Matches fields where the value is less than the specified value
   * @param propertySelector Property selector for the field.
   * @param value The value.
   * @return
   */
  public <TValue> TSelf whereLessThan(Path<? super TValue> propertySelector, TValue value);

  /**
   * Matches fields where the value is less than or equal to the specified value
   * @param fieldName Name of the field.
   * @param value The value.
   * @return
   */
  public TSelf whereLessThanOrEqual (String fieldName, Object value);

  /**
   * Matches fields where the value is less than or equal to the specified value
   * @param propertySelector Property selector for the field.
   * @param value The value.
   * @return
   */
  public <TValue> TSelf whereLessThanOrEqual(Path<? super TValue> propertySelector, TValue value);

  /**
   * Add an AND to the query
   * @return
   */
  public TSelf andAlso();

  /**
   * Add an OR to the query
   * @return
   */
  public TSelf orElse();

  /**
   * Specifies a boost weight to the last where clause.
   * The higher the boost factor, the more relevant the term will be.
   *
   * http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
   * @param boost
   * boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight
   * @return
   */
  public TSelf boost(Double boost);

  /**
   * Specifies a fuzziness factor to the single word term in the last where clause
   *
   * http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
   * @param fuzzy 0.0 to 1.0 where 1.0 means closer match
   * @return
   */
  public TSelf fuzzy (Double fuzzy);

  /**
   * Specifies a proximity distance for the phrase in the last where clause
   *
   * http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
   * @param proximity number of words within
   * @return
   */
  public TSelf proximity (int proximity);

  /**
   * Filter matches to be inside the specified radius
   * @param radius The radius.
   * @param latitude The latitude.
   * @param longitude The longitude.
   * @return
   */
  public TSelf withinRadiusOf(double radius, double latitude, double longitude);

  /**
   * Filter matches to be inside the specified radius
   * @param radius The radius.
   * @param latitude The latitude.
   * @param longitude The longitude.
   * @param radiusUnits The unit of the radius.
   * @return
   */
  public TSelf withinRadiusOf(double radius, double latitude, double longitude, SpatialUnits radiusUnits);

  /**
   * Filter matches to be inside the specified radius
   * @param fieldName The field name for the radius.
   * @param radius The radius.
   * @param latitude The latitude.
   * @param longitude The longitude.
   * @return
   */
  public TSelf withinRadiusOf(String fieldName, double radius, double latitude, double longitude);

  /**
   * Filter matches to be inside the specified radius
   * @param fieldName The field name for the radius.
   * @param radius The radius.
   * @param latitude The latitude.
   * @param longitude The longitude.
   * @param radiusUnits The unit of the radius.
   * @return
   */
  public TSelf withinRadiusOf(String fieldName, double radius, double latitude, double longitude, SpatialUnits radiusUnits);

  /**
   * Filter matches based on a given shape - only documents with the shape defined in fieldName that
   * have a relation rel with the given shapeWKT will be returned
   * @param fieldName The name of the field containg the shape to use for filtering.
   * @param shapeWKT The query shape.
   * @param rel Spatial relation to check
   * @return
   */
  public TSelf relatesToShape(String fieldName, String shapeWKT, SpatialRelation rel);

  /**
   * Filter matches based on a given shape - only documents with the shape defined in fieldName that
   * have a relation rel with the given shapeWKT will be returned
   * @param fieldName The name of the field containg the shape to use for filtering.
   * @param shapeWKT The query shape.
   * @param rel Spatial relation to check
   * @param distanceErrorPct The allowed error percentage.
   * @return
   */
  public TSelf relatesToShape(String fieldName, String shapeWKT, SpatialRelation rel, double distanceErrorPct);

  /**
   * Sorts the query results by distance.
   * @return
   */
  public TSelf sortByDistance();

  /**
   * Order the results by the specified fields
   * The fields are the names of the fields to sort, defaulting to sorting by ascending.
   * You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
   * @param fields The fields.
   * @return
   */
  public TSelf orderBy(String... fields);

  /**
   * Order the results by the specified fields
   * The fields are the names of the fields to sort, defaulting to sorting by ascending.
   * You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
   * @param propertySelectors Property selector for the fields.
   * @return
   */
  public <TValue> TSelf orderBy(Path<?>... propertySelectors);

  /**
   * Order the results by the specified fields
   * The fields are the names of the fields to sort, defaulting to sorting by descending.
   * You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
   * @param fields The fields
   * @return
   */
  public TSelf orderByDescending(String... fields);

  /**
   * Order the results by the specified fields
   * The fields are the names of the fields to sort, defaulting to sorting by descending.
   * You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
   * @param propertySelectors Property selectors for the fields.
   * @return
   */
  public <TValue> TSelf orderByDescending(Path<?>... propertySelectors);

  /**
   * Adds matches highlighting for the specified field.
   *
   * The specified field should be analysed and stored for highlighter to work.
   * For each match it creates a fragment that contains matched text surrounded by highlighter tags.
   * @param fieldName The field name to highlight.
   * @param fragmentLength The fragment length.
   * @param fragmentCount The maximum number of fragments for the field.
   * @param fragmentsField The field in query results item to put highlighing into.
   * @return
   */
  public TSelf highlight(String fieldName, int fragmentLength, int fragmentCount, String fragmentsField);

  /**
   * Adds matches highlighting for the specified field.
   *
   * The specified field should be analysed and stored for highlighter to work.
   * For each match it creates a fragment that contains matched text surrounded by highlighter tags.
   * @param fieldName The field name to highlight.
   * @param fragmentLength The fragment length.
   * @param fragmentCount The fragment count.
   * @param highlightings The maximum number of fragments for the field.
   * @return
   */
  public TSelf highlight(String fieldName, int fragmentLength, int fragmentCount, Reference<FieldHighlightings> highlightings);

  /**
   * Adds matches highlighting for the specified field.
   *
   * The specified field should be analysed and stored for highlighter to work.
   * For each match it creates a fragment that contains matched text surrounded by highlighter tags.
   * @param propertySelector The property to highlight.
   * @param fragmentLength The fragment length.
   * @param fragmentCount The maximum number of fragments for the field.
   * @param fragmentsPropertySelector The property to put highlightings into.
   * @return
   */
  public <TValue> TSelf highlight( Path<?> propertySelector, int fragmentLength, int fragmentCount, ListPath<?, ?> fragmentsPropertySelector);

  /**
   * Adds matches highlighting for the specified field.
   *
   * The specified field should be analysed and stored for highlighter to work.
   * For each match it creates a fragment that contains matched text surrounded by highlighter tags.
   * @param propertySelector The property to highlight.
   * @param fragmentLength The fragment length.
   * @param fragmentCount The maximum number of fragment for the field.
   * @param highlightings
   * @return
   */
  public <TValue> TSelf highlight(Path<?> propertySelector, int fragmentLength, int fragmentCount, Reference<FieldHighlightings> highlightings);

  /**
   * Sets the tags to highlight matches with.
   * @param preTag Prefix tag.
   * @param postTag Postfix tag.
   * @return
   */
  public TSelf setHighlighterTags(String preTag, String postTag);

  /**
   * Sets the tags to highlight matches with.
   * @param preTags Prefix tags.
   * @param postTags Postfix tags.
   * @return
   */
  public TSelf setHighlighterTags(String[] preTags, String[] postTags);

  /**
   * Instructs the query to wait for non stale results as of now.
   * @return
   */
  public TSelf waitForNonStaleResultsAsOfNow();

  /**
   * Instructs the query to wait for non stale results as of the last write made by any session belonging to the
   * current document store.
   *
   * This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
   * However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results.
   * @return
   */
  public TSelf waitForNonStaleResultsAsOfLastWrite();

  /**
   * Instructs the query to wait for non stale results as of the last write made by any session belonging to the
   * current document store.
   *
   * This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
   * However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results.
   * @param waitTimeout
   * @return
   */
  public TSelf waitForNonStaleResultsAsOfLastWrite(long waitTimeout);

  /**
   * Instructs the query to wait for non stale results as of now for the specified timeout.
   * @param waitTimeout The wait timeout.
   * @return
   */
  public TSelf waitForNonStaleResultsAsOfNow(long waitTimeout);

  /**
   * Instructs the query to wait for non stale results as of the cutoff date.
   * @param cutOff The cut off.
   * @return
   */
  public TSelf waitForNonStaleResultsAsOf(Date cutOff);

  /**
   * Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
   * @param cutOff The cut off.
   * @param waitTimeout The wait timeout.
   * @return
   */
  public TSelf waitForNonStaleResultsAsOf(Date cutOff, long waitTimeout);

  /**
   * Instructs the query to wait for non stale results as of the cutoff etag.
   * @param cutOffEtag The cut off etag.
   * @return
   */
  public TSelf waitForNonStaleResultsAsOf(Etag cutOffEtag);

  /**
   * Instructs the query to wait for non stale results as of the cutoff etag for the specified timeout.
   * @param cutOffEtag the cut off etag.
   * @param waitTimeout The wait timeout.
   * @return
   */
  public TSelf waitForNonStaleResultsAsOf(Etag cutOffEtag, long waitTimeout);

  /**
   * EXPERT ONLY: Instructs the query to wait for non stale results.
   * This shouldn't be used outside of unit tests unless you are well aware of the implications
   * @return
   */
  public TSelf waitForNonStaleResults();

  /**
   * Allows you to modify the index query before it is sent to the server
   * @param beforeQueryExecution
   * @return
   */
  public TSelf beforeQueryExecution(Action1<IndexQuery> beforeQueryExecution);

  /**
   * EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
   * This shouldn't be used outside of unit tests unless you are well aware of the implications
   * @param waitTimeout The wait timeout.
   * @return
   */
  public TSelf waitForNonStaleResults(long waitTimeout);

  /**
   * Order the search results randomly
   * @return
   */
  public TSelf randomOrdering();

  /**
   * Order the search results randomly using the specified seed
   * this is useful if you want to have repeatable random queries
   * @param seed
   * @return
   */
  public TSelf randomOrdering(String seed);

  /**
   * Adds an ordering for a specific field to the query
   * @param fieldName Name of the field.
   * @param descending If set to true [descending]
   * @return
   */
  public TSelf addOrder(String fieldName, boolean descending);

  /**
   * Adds an ordering for a specific field to the query
   * @param propertySelector Property selector for the field.
   * @param descending If set to true [descending]
   * @return
   */
  public <TValue> TSelf addOrder(Path<?> propertySelector, boolean descending);

  /**
   * Adds an ordering for a specific field to the query and specifies the type of field for sorting purposes
   * @param fieldName Name of the field.
   * @param descending if set to true [descending]
   * @param fieldType The type of the field to be sorted.
   * @return
   */
  public TSelf addOrder (String fieldName, boolean descending, Class<?> fieldType);

  /**
   * Simplified method for opening a new clause within the query
   * @return
   */
  public TSelf openSubclause();

  /**
   * Simplified method for closing a clause within the query
   * @return
   */
  public TSelf closeSubclause();

  /**
   * Perform a search for documents which fields that match the searchTerms.
   * If there is more than a single term, each of them will be checked independently.
   * @param fieldName
   * @param searchTerms
   * @return
   */
  public TSelf search(String fieldName, String searchTerms);

  /**
   * Perform a search for documents which fields that match the searchTerms.
   * If there is more than a single term, each of them will be checked independently.
   * @param propertySelector
   * @param searchTerms
   * @return
   */
  public <TValue> TSelf search(Path<?> propertySelector, String searchTerms);

  /**
   * Partition the query so we can intersect different parts of the query
   * across different index entries.
   * @return
   */
  public TSelf intersect();

  /**
   * Callback to get the results of the query
   * @param afterQueryExecuted
   */
  public void afterQueryExecuted(Action1<QueryResult> afterQueryExecuted);

  /**
   * Called externally to raise the after query executed callback
   * @param result
   */
  public void invokeAfterQueryExecuted(QueryResult result);

  /**
   * Provide statistics about the query, such as total count of matching records
   * @param stats
   * @return
   */
  public TSelf statistics(Reference<RavenQueryStatistics> stats);

  /**
   * Select the default field to use for this query
   * @param field
   * @return
   */
  public TSelf usingDefaultField(String field);

  /**
   * Select the default operator to use for this query
   * @param queryOperator
   * @return
   */
  public TSelf usingDefaultOperator(QueryOperator queryOperator);

  /**
   * Disables tracking for queried entities by Raven's Unit of Work.
   * Usage of this option will prevent holding query results in memory.
   * @return
   */
  public TSelf noTracking();

  /**
   * Disables caching for query results.
   * @return
   */
  public TSelf noCaching();

  /**
   * Apply distinct operation to this query
   * @return
   */
  public TSelf distinct();

  /**
   * Sets a transformer to use after executing a query
   * @param resultsTransformer
   * @return
   */
  public TSelf setResultTransformer(String resultsTransformer);

  /**
   * Adds an ordering by score for a specific field to the query
   * @return
   */
  public TSelf orderByScore();

  /**
   * Adds an ordering by score for a specific field to the query
   * @return
   */
  public TSelf orderByScoreDescending();

  /**
   * Adds explanations of scores calculated for queried documents to the query result
   * @return
   */
  public TSelf explainScores();
}
