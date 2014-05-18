package net.ravendb.client.document;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.basic.Tuple;
import net.ravendb.client.EscapeQueryOptions;
import net.ravendb.client.FieldHighlightings;
import net.ravendb.client.IDocumentQuery;
import net.ravendb.client.WhereParams;

import com.mysema.query.types.Path;


/**
 * Mostly used by the linq provider
 * @param <T>
 */
public interface IAbstractDocumentQuery<T> {
  /**
   *  Get the name of the index being queried
   */
  public String getIndexQueried();

  /**
   * Gets the document convention from the query session
   * @return
   */
  public DocumentConvention getDocumentConvention();

  /**
   *  Instruct the query to wait for non stale result for the specified wait timeout.
   * @param waitTimeout The wait timeout in milis
   */
  public IDocumentQuery<T> waitForNonStaleResults(long waitTimeout);

  /**
   * Gets the fields for projection
   * @return
   */
  public Collection<String> getProjectionFields();

  /**
   *  Order the search results randomly
   * @return
   */
  public IDocumentQuery<T> randomOrdering();

  /**
   * Order the search results randomly using the specified seed
   * this is useful if you want to have repeatable random queries
   * @param seed
   * @return
   */
  public IDocumentQuery<T> randomOrdering(String seed);

  /**
   * Adds an ordering for a specific field to the query
   * @param fieldName Name of the field.
   * @param descending If set to true [descending]
   */
  public IDocumentQuery<T> addOrder(String fieldName, boolean descending);

  /**
   * Adds an ordering for a specific field to the query and specifies the type of field for sorting purposes
   * @param fieldName Name of the field.
   * @param descending If set to true [descending]
   * @param fieldType the type of the field to be sorted
   */
  public IDocumentQuery<T> addOrder(String fieldName, boolean descending, Class<?> fieldType);

  /**
   * Includes the specified path in the query, loading the document specified in that path
   * @param path
   * @return
   */
  public IDocumentQuery<T> include(Path<?> path);

  /**
   * Includes the specified path in the query, loading the document specified in that path
   * @param path
   * @return
   */
  public IDocumentQuery<T> include(String path);

  /**
   * Includes the specified path in the query, loading the document specified in that path
   * @param path
   * @return
   */
  public IDocumentQuery<T> include(Class<?> targetClass, Path<?> path);

  /**
   * Takes the specified count.
   * @param count The count.
   * @return
   */
  public IDocumentQuery<T> take(int count);

  /**
   * Skips the specified count.
   * @param count The count.
   * @return
   */
  public IDocumentQuery<T> skip(int count);

  /**
   * Filter the results from the index using the specified where clause.
   * @param whereClause The where clause.
   * @return
   */
  public IDocumentQuery<T> where(String whereClause);

  /**
   * Matches exact value
   * Defaults to NotAnalyzed
   * @param fieldName
   * @param value
   * @return
   */
  public IDocumentQuery<T> whereEquals(String fieldName, Object value);


  /**
   * Matches exact value
   *
   * Default to allow wildcard only if analyzed
   * @param fieldName
   * @param value
   * @param isAnalyzed
   * @return
   */
  public IDocumentQuery<T> whereEquals(String fieldName, Object value, boolean isAnalyzed);

  /**
   * Simplified method for opening a new clause within the query
   * @return
   */
  public IDocumentQuery<T> openSubclause();

  /**
   * Simplified method for closing a clause within the query
   * @return
   */
  public IDocumentQuery<T> closeSubclause();


  /**
   * Matches exact value
   * @param whereParams
   * @return
   */
  public IDocumentQuery<T> whereEquals (WhereParams whereParams);

  /**
   * Negate the next operation
   */
  public void negateNext();

  /**
   * Check that the field has one of the specified value
   * @param fieldName
   * @param values
   * @return
   */
  public IDocumentQuery<T> whereIn(String fieldName, Collection<?> values);

  /**
   * Matches fields which starts with the specified value.
   * @param fieldName Name of the field.
   * @param value The value.
   * @return
   */
  public IDocumentQuery<T> whereStartsWith(String fieldName, Object value);

  /**
   * Matches fields which ends with the specified value.
   * @param fieldName Name of the field.
   * @param value The value.
   * @return
   */
  public IDocumentQuery<T> whereEndsWith (String fieldName, Object value);

  /**
   * Matches fields where the value is between the specified start and end, exclusive
   * @param fieldName Name of the field.
   * @param start The start.
   * @param end The end.
   * @return
   */
  public IDocumentQuery<T> whereBetween (String fieldName, Object start, Object end);

  /**
   * Matches fields where the value is between the specified start and end, inclusive
   * @param fieldName Name of the field.
   * @param start The start.
   * @param end The end.
   * @return
   */
  public IDocumentQuery<T> whereBetweenOrEqual (String fieldName, Object start, Object end);

  /**
   * Matches fields where the value is greater than the specified value
   * @param fieldName Name of the field.
   * @param value The value.
   * @return
   */
  public IDocumentQuery<T> whereGreaterThan (String fieldName, Object value);

  /**
   * Matches fields where the value is greater than or equal to the specified value
   * @param fieldName Name of the field.
   * @param value The value.
   * @return
   */
  public IDocumentQuery<T> whereGreaterThanOrEqual (String fieldName, Object value);

  /**
   * Matches fields where the value is less than the specified value
   * @param fieldName Name of the field.
   * @param value The value.
   * @return
   */
  public IDocumentQuery<T> whereLessThan (String fieldName, Object value);

  /**
   * Matches fields where the value is less than or equal to the specified value
   * @param fieldName Name of the field.
   * @param value The value.
   * @return
   */
  public IDocumentQuery<T> whereLessThanOrEqual (String fieldName, Object value);

  /**
   * Add an AND to the query
   * @return
   */
  public IDocumentQuery<T> andAlso();

  /**
   * Add an OR to the query
   * @return
   */
  public IDocumentQuery<T> orElse();

  /**
   * Specifies a boost weight to the last where clause.
   * The higher the boost factor, the more relevant the term will be.
   *
   * http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
   * @param boost
   * boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight
   * @return
   */
  public IDocumentQuery<T> boost(Double boost);

  /**
   * Specifies a fuzziness factor to the single word term in the last where clause
   *
   * http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
   * @param fuzzy 0.0 to 1.0 where 1.0 means closer match
   * @return
   */
  public IDocumentQuery<T> fuzzy (Double fuzzy);

  /**
   * Specifies a proximity distance for the phrase in the last where clause
   *
   * http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
   * @param proximity number of words within
   * @return
   */
  public IDocumentQuery<T> proximity (int proximity);

  /**
   * Order the results by the specified fields
   * The fields are the names of the fields to sort, defaulting to sorting by ascending.
   * You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
   * @param fields The fields.
   * @return
   */
  public IDocumentQuery<T> orderBy(String... fields);

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
  public IDocumentQuery<T> highlight(String fieldName, int fragmentLength, int fragmentCount, String fragmentsField);

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
  public IDocumentQuery<T> highlight(String fieldName, int fragmentLength, int fragmentCount, Reference<FieldHighlightings> highlightings);

  /**
   * Sets the tags to highlight matches with.
   * @param preTag Prefix tag.
   * @param postTag Postfix tag.
   * @return
   */
  public IDocumentQuery<T> setHighlighterTags(String preTag, String postTag);

  /**
   * Sets the tags to highlight matches with.
   * @param preTags Prefix tags.
   * @param postTags Postfix tags.
   * @return
   */
  public IDocumentQuery<T> setHighlighterTags(String[] preTags, String[] postTags);

  /**
   * Instructs the query to wait for non stale results as of now.
   * @return
   */
  public IDocumentQuery<T> waitForNonStaleResultsAsOfNow();

  /**
   * Instructs the query to wait for non stale results as of now for the specified timeout.
   * @param waitTimeout The wait timeout.
   * @return
   */
  public IDocumentQuery<T> waitForNonStaleResultsAsOfNow(long waitTimeout);

  /**
   * Instructs the query to wait for non stale results as of the cutoff date.
   * @param cutOff The cut off.
   * @return
   */
  public IDocumentQuery<T> waitForNonStaleResultsAsOf(Date cutOff);

  /**
   * Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
   * @param cutOff The cut off.
   * @param waitTimeout The wait timeout.
   * @return
   */
  public IDocumentQuery<T> waitForNonStaleResultsAsOf(Date cutOff, long waitTimeout);

  /**
   * EXPERT ONLY: Instructs the query to wait for non stale results.
   * This shouldn't be used outside of unit tests unless you are well aware of the implications
   * @return
   */
  public IDocumentQuery<T> waitForNonStaleResults();

  /**
   * Perform a search for documents which fields that match the searchTerms.
   * If there is more than a single term, each of them will be checked independently.
   * @param fieldName
   * @param searchTerms
   * @return
   */
  public IDocumentQuery<T> search(String fieldName, String searchTerms);

  /**
   * Perform a search for documents which fields that match the searchTerms.
   * If there is more than a single term, each of them will be checked independently.
   * @param fieldName
   * @param searchTerms
   * @return
   */
  public IDocumentQuery<T> search(String fieldName, String searchTerms, EscapeQueryOptions escapeQueryOptions);

  /**
   *  The last term that we asked the query to use equals on
   * @return
   */
  public Tuple<String, String> getLastEqualityTerm();

  public IDocumentQuery<T> intersect();

  public void addRootType(Class<T> type);

  public Iterator<T> iterator();

  public IDocumentQuery<T> distinct();

  /**
   * Performs a query matching ANY of the provided values against the given field (OR)
   * @param fieldName
   * @param values
   */
  public IDocumentQuery<T> containsAny(String fieldName, Collection<Object> values);

  /**
   * Performs a query matching ALL of the provided values against the given field (AND)
   * @param fieldName
   * @param values
   */
  public IDocumentQuery<T> containsAll(String fieldName, Collection<Object> values);

}
