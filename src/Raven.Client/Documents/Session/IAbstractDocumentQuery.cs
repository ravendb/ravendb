using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session
{
    ///<summary>
    /// Mostly used by the linq provider
    ///</summary>
    public interface IAbstractDocumentQuery<T>
    {
        string IndexName { get; }

        string CollectionName { get; }

        /// <summary>
        /// Gets the document convention from the query session
        /// </summary>
        DocumentConventions Conventions { get; }

        /// <summary>
        /// Determines if it is a dynamic map-reduce query
        /// </summary>
        bool IsDynamicMapReduce { get; }

        /// <summary>
        ///   Instruct the query to wait for non stale result for the specified wait timeout.
        /// </summary>
        /// <param name = "waitTimeout">The wait timeout.</param>
        /// <returns></returns>
        void WaitForNonStaleResults(TimeSpan waitTimeout);

        /// <summary>
        ///   Gets the fields for projection
        /// </summary>
        /// <returns></returns>
        IEnumerable<string> GetProjectionFields();

        /// <summary>
        /// Order the search results randomly
        /// </summary>
        void RandomOrdering();

        /// <summary>
        /// Order the search results randomly using the specified seed
        /// this is useful if you want to have repeatable random queries
        /// </summary>
        void RandomOrdering(string seed);

        /// <summary>
        /// Sort using custom sorter on the server
        /// </summary>
        void CustomSortUsing(string typeName, bool descending = false);

        /// <summary>
        ///   Adds an ordering for a specific field to the query
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "descending">if set to <c>true</c> [descending].</param>
        void AddOrder(string fieldName, bool descending, OrderingType ordering = OrderingType.String);

        /// <summary>
        ///   Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name = "path">The path.</param>
        void Include(string path);

        /// <summary>
        ///   Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name = "path">The path.</param>
        void Include(Expression<Func<T, object>> path);

        /// <summary>
        ///   Takes the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        /// <returns></returns>
        void Take(int count);

        /// <summary>
        ///   Skips the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        /// <returns></returns>
        void Skip(int count);

        /// <summary>
        ///   Matches value
        /// </summary>
        void WhereEquals(string fieldName, object value, bool exact = false);

        /// <summary>
        ///   Matches value
        /// </summary>
        void WhereEquals(WhereParams whereParams);

        /// <summary>
        ///   Simplified method for opening a new clause within the query
        /// </summary>
        /// <returns></returns>
        void OpenSubclause();

        /// <summary>
        ///   Simplified method for closing a clause within the query
        /// </summary>
        /// <returns></returns>
        void CloseSubclause();

        ///<summary>
        /// Negate the next operation
        ///</summary>
        void NegateNext();

        /// <summary>
        /// Check that the field has one of the specified value
        /// </summary>
        void WhereIn(string fieldName, IEnumerable<object> values, bool exact = false);

        /// <summary>
        ///   Matches fields which starts with the specified value.
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        void WhereStartsWith(string fieldName, object value);

        /// <summary>
        ///   Matches fields which ends with the specified value.
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        void WhereEndsWith(string fieldName, object value);

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, exclusive
        /// </summary>
        void WhereBetween(string fieldName, object start, object end, bool exact = false);

        /// <summary>
        ///   Matches fields where the value is greater than the specified value
        /// </summary>
        void WhereGreaterThan(string fieldName, object value, bool exact = false);

        /// <summary>
        ///   Matches fields where the value is greater than or equal to the specified value
        /// </summary>
        void WhereGreaterThanOrEqual(string fieldName, object value, bool exact = false);

        /// <summary>
        ///   Matches fields where the value is less than the specified value
        /// </summary>
        void WhereLessThan(string fieldName, object value, bool exact = false);

        /// <summary>
        ///   Matches fields where the value is less than or equal to the specified value
        /// </summary>
        void WhereLessThanOrEqual(string fieldName, object value, bool exact = false);

        void WhereExists(string fieldName);

        /// <summary>
        ///   Add an AND to the query
        /// </summary>
        void AndAlso();

        /// <summary>
        ///   Add an OR to the query
        /// </summary>
        void OrElse();

        /// <summary>
        ///   Specifies a boost weight to the last where clause.
        ///   The higher the boost factor, the more relevant the term will be.
        /// </summary>
        /// <param name = "boost">boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight</param>
        /// <returns></returns>
        /// <remarks>
        ///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
        /// </remarks>
        void Boost(decimal boost);

        /// <summary>
        ///   Specifies a fuzziness factor to the single word term in the last where clause
        /// </summary>
        /// <param name = "fuzzy">0.0 to 1.0 where 1.0 means closer match</param>
        /// <returns></returns>
        /// <remarks>
        ///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
        /// </remarks>
        void Fuzzy(decimal fuzzy);

        /// <summary>
        ///   Specifies a proximity distance for the phrase in the last where clause
        /// </summary>
        /// <param name = "proximity">number of words within</param>
        /// <returns></returns>
        /// <remarks>
        ///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
        /// </remarks>
        void Proximity(int proximity);

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name = "fields">The fields.</param>
        void OrderBy(string field, OrderingType ordering = OrderingType.String);

        void OrderByScore();

        void OrderByScoreDescending();

        /// <summary>
        ///   Adds matches highlighting for the specified field.
        /// </summary>
        /// <remarks>
        ///   The specified field should be analyzed and stored for highlighter to work.
        ///   For each match it creates a fragment that contains matched text surrounded by highlighter tags.
        /// </remarks>
        /// <param name="fieldName">The field name to highlight.</param>
        /// <param name="fragmentLength">The fragment length.</param>
        /// <param name="fragmentCount">The maximum number of fragments for the field.</param>
        /// <param name="fragmentsField">The field in query results item to put highlights into.</param>
        void Highlight(string fieldName, int fragmentLength, int fragmentCount, string fragmentsField);

        /// <summary>
        ///   Adds matches highlighting for the specified field.
        /// </summary>
        /// <remarks>
        ///   The specified field should be analyzed and stored for highlighter to work.
        ///   For each match it creates a fragment that contains matched text surrounded by highlighter tags.
        /// </remarks>
        /// <param name="fieldName">The field name to highlight.</param>
        /// <param name="fragmentLength">The fragment length.</param>
        /// <param name="fragmentCount">The maximum number of fragments for the field.</param>
        /// <param name="highlightings">Field highlights for all results.</param>
        void Highlight(string fieldName, int fragmentLength, int fragmentCount, out FieldHighlightings highlightings);

        /// <summary>
        ///   Adds matches highlighting for the specified field on a Map/Reduce Index.
        /// </summary>
        /// <remarks>
        ///   This is only valid for Map/Reduce Index queries.
        ///   The specified field and key should be analyzed and stored for highlighter to work.
        ///   For each match it creates a fragment that contains matched text surrounded by highlighter tags.
        /// </remarks>
        /// <param name="fieldName">The field name to highlight.</param>
        /// <param name="fieldKeyName">The field key to associate highlights with.</param>
        /// <param name="fragmentLength">The fragment length.</param>
        /// <param name="fragmentCount">The maximum number of fragments for the field.</param>
        /// <param name="highlightings">Field highlights for all results.</param>
        void Highlight(string fieldName, string fieldKeyName, int fragmentLength, int fragmentCount, out FieldHighlightings highlightings);

        /// <summary>
        ///   Sets the tags to highlight matches with.
        /// </summary>
        /// <param name="preTag">Prefix tag.</param>
        /// <param name="postTag">Postfix tag.</param>
        void SetHighlighterTags(string preTag, string postTag);

        /// <summary>
        ///   Sets the tags to highlight matches with.
        /// </summary>
        /// <param name="preTags">Prefix tags.</param>
        /// <param name="postTags">Postfix tags.</param>
        void SetHighlighterTags(string[] preTags, string[] postTags);

        /// <summary>
        ///   Instructs the query to wait for non stale results as of now.
        /// </summary>
        /// <returns></returns>
        void WaitForNonStaleResultsAsOfNow();

        /// <summary>
        ///   Instructs the query to wait for non stale results as of now for the specified timeout.
        /// </summary>
        /// <param name = "waitTimeout">The wait timeout.</param>
        /// <returns></returns>
        void WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout);

        /// <summary>
        ///   EXPERT ONLY: Instructs the query to wait for non stale results.
        ///   This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        void WaitForNonStaleResults();

        /// <summary>
        /// Perform a search for documents which fields that match the searchTerms.
        /// If there is more than a single term, each of them will be checked independently.
        /// </summary>
        void Search(string fieldName, string searchTerms, SearchOperator @operator = SearchOperator.Or);

        /// <summary>
        ///   Returns a <see cref = "System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        ///   A <see cref = "System.String" /> that represents this instance.
        /// </returns>
        string ToString();

        /// <summary>
        ///   The last term that we asked the query to use equals on
        /// </summary>
        KeyValuePair<string, object> GetLastEqualityTerm(bool isAsync = false);

        void Intersect();
        void AddRootType(Type type);
        void Distinct();

        /// <summary>
        /// Performs a query matching ANY of the provided values against the given field (OR)
        /// </summary>
        void ContainsAny(string fieldName, IEnumerable<object> values);

        /// <summary>
        /// Performs a query matching ALL of the provided values against the given field (AND)
        /// </summary>
        void ContainsAll(string fieldName, IEnumerable<object> values);

        void GroupBy(string fieldName, params string[] fieldNames);

        void GroupByKey(string fieldName, string projectedName = null);

        void GroupBySum(string fieldName, string projectedName = null);

        void GroupByCount(string projectedName = null);

        void WhereTrue();
    }
}
