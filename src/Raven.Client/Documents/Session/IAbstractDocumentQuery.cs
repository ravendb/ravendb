using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Queries.Spatial;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Client.Documents.Session.Loaders;

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
        ///   Instruct the query to wait for non stale results.
        ///   This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        /// <param name = "waitTimeout">Maximum time to wait for index query results to become non-stale before exception is thrown. Default: 15 seconds.</param>
        void WaitForNonStaleResults(TimeSpan? waitTimeout = null);

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

#if FEATURE_CUSTOM_SORTING
        /// <summary>
        /// Sort using custom sorter on the server
        /// </summary>
        void CustomSortUsing(string typeName, bool descending = false);
#endif

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
        ///   Includes the specified documents and/or counters in the query, specified by IncludeBuilder
        /// </summary>
        /// <param name="includes"></param>
        void Include(IncludeBuilder includes);

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
        ///   Matches evaluated method
        /// </summary>
        void WhereEquals(string fieldName, MethodCall method, bool exact = false);

        /// <summary>
        ///   Matches value
        /// </summary>
        void WhereEquals(WhereParams whereParams);

        /// <summary>
        ///   Not matches value
        /// </summary>
        void WhereNotEquals(string fieldName, object value, bool exact = false);

        /// <summary>
        ///   Not matches evaluated method
        /// </summary>
        void WhereNotEquals(string fieldName, MethodCall method, bool exact = false);

        /// <summary>
        ///   Not matches value
        /// </summary>
        void WhereNotEquals(WhereParams whereParams);

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
        void WhereStartsWith(string fieldName, object value, bool exact = false);

        /// <summary>
        ///   Matches fields which ends with the specified value.
        /// </summary>
        void WhereEndsWith(string fieldName, object value, bool exact = false);

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, inclusive
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
        ///   Matches fields where Regex.IsMatch(filed, pattern)
        /// </summary>
        void WhereRegex(string fieldName, string pattern);

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

        void OrderBy(string field, string sorterName);

        /// <summary>
        ///   Order the results by the specified fields
        ///   The field is the name of the field to sort, defaulting to sorting by ascending.
        /// </summary>
        /// <param name = "field">The fields.</param>
        void OrderBy(string field, OrderingType ordering = OrderingType.String);

        void OrderByDescending(string field, string sorterName);

        void OrderByDescending(string field, OrderingType ordering = OrderingType.String);

        void OrderByScore();

        void OrderByScoreDescending();

        void Highlight(string fieldName, int fragmentLength, int fragmentCount, HighlightingOptions options, out Highlightings highlightings);

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

        void GroupBy((string Name, GroupByMethod Method) field, params (string Name, GroupByMethod Method)[] fields);

        void GroupByKey(string fieldName, string projectedName = null);

        void GroupBySum(string fieldName, string projectedName = null);

        void GroupByCount(string projectedName = null);

        void WhereTrue();

        void Spatial(DynamicSpatialField field, SpatialCriteria criteria);

        void Spatial(string fieldName, SpatialCriteria criteria);

        void OrderByDistance(DynamicSpatialField field, double latitude, double longitude);

        void OrderByDistance(string fieldName, double latitude, double longitude);

        void OrderByDistance(DynamicSpatialField field, string shapeWkt);

        void OrderByDistance(string fieldName, string shapeWkt);

        void OrderByDistanceDescending(DynamicSpatialField field, double latitude, double longitude);

        void OrderByDistanceDescending(string fieldName, double latitude, double longitude);

        void OrderByDistanceDescending(DynamicSpatialField field, string shapeWkt);

        void OrderByDistanceDescending(string fieldName, string shapeWkt);

        MoreLikeThisScope MoreLikeThis();

        void AggregateBy(FacetBase facet);

        void AggregateUsing(string facetSetupDocumentId);

        void AddFromAliasToWhereTokens(string fromAlias);

        string AddAliasToCounterIncludesTokens(string fromAlias);

        string ProjectionParameter(object id);

        void SuggestUsing(SuggestionBase suggestion);

    }
}
