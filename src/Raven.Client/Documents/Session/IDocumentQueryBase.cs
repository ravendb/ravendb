using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Queries.Spatial;
using Raven.Client.Documents.Queries.Timings;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public interface IQueryBase<T, out TSelf>
        where TSelf : IQueryBase<T, TSelf>
    {
        /// <summary>
        ///     Gets the document convention from the query session
        /// </summary>
        DocumentConventions Conventions { get; }

        /// <summary>
        ///     Callback to get the results of the query
        /// </summary>
        void AfterQueryExecuted(Action<QueryResult> action);


        /// <summary>
        ///     Callback to get the results of the stream
        /// </summary>
        void AfterStreamExecuted(Action<BlittableJsonReaderObject> action);

        /// <summary>
        ///     Allows you to modify the index query before it is sent to the server
        /// </summary>
        TSelf BeforeQueryExecuted(Action<IndexQuery> beforeQueryExecuted);

        /// <summary>
        ///     Called externally to raise the after query executed callback
        /// </summary>
        void InvokeAfterQueryExecuted(QueryResult result);

        /// <summary>
        ///     Called externally to raise the after query executed callback
        /// </summary>
        void InvokeAfterStreamExecuted(BlittableJsonReaderObject result);

        /// <summary>
        ///     Disables caching for query results.
        /// </summary>
        TSelf NoCaching();

        /// <summary>
        ///     Disables tracking for queried entities by Raven's Unit of Work.
        ///     Usage of this option will prevent holding query results in memory.
        /// </summary>
        TSelf NoTracking();

        /// <summary>
        ///     Enables calculation of timings for various parts of a query (Lucene search, loading documents, transforming
        ///     results). Default: false
        /// </summary>
        TSelf Timings(out QueryTimings timings);

        /// <summary>
        ///     Skips the specified count.
        /// </summary>
        /// <param name="count">Number of items to skip.</param>
        TSelf Skip(int count);

        /// <summary>
        ///     Provide statistics about the query, such as total count of matching records
        /// </summary>
        TSelf Statistics(out QueryStatistics stats);

        /// <summary>
        ///     Takes the specified count.
        /// </summary>
        /// <param name="count">Maximum number of items to take.</param>
        TSelf Take(int count);

        /// <summary>
        ///     Select the default operator to use for this query
        /// </summary>
        TSelf UsingDefaultOperator(QueryOperator queryOperator);

        /// <summary>
        ///   Instruct the query to wait for non stale results.
        ///   This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        /// <param name = "waitTimeout">Maximum time to wait for index query results to become non-stale before exception is thrown. Default: 15 seconds.</param>
        TSelf WaitForNonStaleResults(TimeSpan? waitTimeout = null);

        /// <summary>
        ///     Create the index query object for this query
        /// </summary>
        IndexQuery GetIndexQuery();

        /// <summary>
        /// Add a named parameter to the query
        /// </summary>
        TSelf AddParameter(string name, object value);
    }

    public interface IFilterDocumentQueryBase<T, TSelf> where TSelf : IDocumentQueryBase<T, TSelf>
    {
        /// <summary>
        ///     Negate the next operation
        /// </summary>
        TSelf Not { get; }

        /// <summary>
        ///     Add an AND to the query
        /// </summary>
        TSelf AndAlso();

        /// <summary>
        ///     Simplified method for closing a clause within the query
        /// </summary>
        /// <returns></returns>
        TSelf CloseSubclause();

        /// <summary>
        ///     Performs a query matching ALL of the provided values against the given field (AND)
        /// </summary>
        TSelf ContainsAll(string fieldName, IEnumerable<object> values);

        /// <summary>
        ///     Performs a query matching ALL of the provided values against the given field (AND)
        /// </summary>
        TSelf ContainsAll<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values);

        /// <summary>
        ///     Performs a query matching ANY of the provided values against the given field (OR)
        /// </summary>
        TSelf ContainsAny(string fieldName, IEnumerable<object> values);

        /// <summary>
        ///     Performs a query matching ANY of the provided values against the given field (OR)
        /// </summary>
        TSelf ContainsAny<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values);

        /// <summary>
        ///     Negate the next operation
        /// </summary>
        void NegateNext();

        /// <summary>
        ///     Simplified method for opening a new clause within the query
        /// </summary>
        /// <returns></returns>
        TSelf OpenSubclause();

        /// <summary>
        ///     Add an OR to the query
        /// </summary>
        TSelf OrElse();

        /// <summary>
        ///     Perform a search for documents which fields that match the searchTerms.
        ///     If there is more than a single term, each of them will be checked independently.
        /// </summary>
        /// <param name="fieldName">Marks a field in which terms should be looked for</param>
        /// <param name="searchTerms">
        ///     Space separated terms e.g. 'John Adam' means that we will look in selected field for 'John'
        ///     or 'Adam'.
        /// </param>
        TSelf Search(string fieldName, string searchTerms, SearchOperator @operator = SearchOperator.Or);

        /// <summary>
        ///     Perform a search for documents which fields that match the searchTerms.
        ///     If there is more than a single term, each of them will be checked independently.
        /// </summary>
        /// <param name="propertySelector">Expression marking a field in which terms should be looked for</param>
        /// <param name="searchTerms">
        ///     Space separated terms e.g. 'John Adam' means that we will look in selected field for 'John'
        ///     or 'Adam'.
        /// </param>
        TSelf Search<TValue>(Expression<Func<T, TValue>> propertySelector, string searchTerms, SearchOperator @operator = SearchOperator.Or);

        /// <summary>
        ///     This function exists solely to forbid in memory where clause on IDocumentQuery, because
        ///     that is nearly always a mistake.
        /// </summary>
        IEnumerable<T> Where(Func<T, bool> predicate);

        /// <summary>
        ///     Filter the results from the index using the specified where clause.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="whereClause">Lucene-syntax based query predicate.</param>
        TSelf WhereLucene(string fieldName, string whereClause);

        /// <summary>
        ///     Filter the results from the index using the specified where clause.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="whereClause">Lucene-syntax based query predicate.</param>
        TSelf WhereLucene(string fieldName, string whereClause, bool exact);

        /// <summary>
        ///     Matches fields where the value is between the specified start and end, inclusive 
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="start">The start.</param>
        /// <param name="end">The end.</param>
        TSelf WhereBetween(string fieldName, object start, object end, bool exact = false);

        /// <summary>
        ///     Matches fields where the value is between the specified start and end, inclusive
        /// </summary>
        /// <param name="propertySelector">Property selector for the field.</param>
        /// <param name="start">The start.</param>
        /// <param name="end">The end.</param>
        TSelf WhereBetween<TValue>(Expression<Func<T, TValue>> propertySelector, TValue start, TValue end, bool exact = false);

        /// <summary>
        ///     Matches fields which ends with the specified value.
        /// </summary>
        TSelf WhereEndsWith(string fieldName, object value, bool exact = false);

        /// <summary>
        ///     Matches fields which ends with the specified value.
        /// </summary>
        TSelf WhereEndsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact = false);

        /// <summary>
        ///     Matches value
        /// </summary>
        TSelf WhereEquals(string fieldName, object value, bool exact = false);

        /// <summary>
        ///     Matches the evaluated expression
        /// </summary>
        TSelf WhereEquals(string fieldName, MethodCall value, bool exact = false);

        /// <summary>
        ///     Matches value
        /// </summary>
        TSelf WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact = false);

        /// <summary>
        ///     Matches value
        /// </summary>
        TSelf WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, MethodCall value, bool exact = false);

        /// <summary>
        ///     Matches value
        /// </summary>
        TSelf WhereEquals(WhereParams whereParams);

        /// <summary>
        ///     Not matches value
        /// </summary>
        TSelf WhereNotEquals(string fieldName, object value, bool exact = false);

        /// <summary>
        ///     Not matches the evaluated expression
        /// </summary>
        TSelf WhereNotEquals(string fieldName, MethodCall value, bool exact = false);

        /// <summary>
        ///     Not matches value
        /// </summary>
        TSelf WhereNotEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact = false);

        /// <summary>
        ///     Matches value
        /// </summary>
        TSelf WhereNotEquals<TValue>(Expression<Func<T, TValue>> propertySelector, MethodCall value, bool exact = false);

        /// <summary>
        ///     Not matches value
        /// </summary>
        TSelf WhereNotEquals(WhereParams whereParams);

        /// <summary>
        ///     Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        TSelf WhereGreaterThan(string fieldName, object value, bool exact = false);

        /// <summary>
        ///     Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name="propertySelector">Property selector for the field.</param>
        /// <param name="value">The value.</param>
        TSelf WhereGreaterThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact = false);

        /// <summary>
        ///     Matches fields where the value is greater than or equal to the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        TSelf WhereGreaterThanOrEqual(string fieldName, object value, bool exact = false);

        /// <summary>
        ///     Matches fields where the value is greater than or equal to the specified value
        /// </summary>
        /// <param name="propertySelector">Property selector for the field.</param>
        /// <param name="value">The value.</param>
        TSelf WhereGreaterThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact = false);

        /// <summary>
        ///     Check that the field has one of the specified values
        /// </summary>
        TSelf WhereIn(string fieldName, IEnumerable<object> values, bool exact = false);

        /// <summary>
        ///     Check that the field has one of the specified values
        /// </summary>
        TSelf WhereIn<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values, bool exact = false);

        /// <summary>
        ///     Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        TSelf WhereLessThan(string fieldName, object value, bool exact = false);

        /// <summary>
        ///     Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name="propertySelector">Property selector for the field.</param>
        /// <param name="value">The value.</param>
        TSelf WhereLessThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact = false);

        /// <summary>
        ///     Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        TSelf WhereLessThanOrEqual(string fieldName, object value, bool exact = false);

        /// <summary>
        ///     Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name="propertySelector">Property selector for the field.</param>
        /// <param name="value">The value.</param>
        TSelf WhereLessThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact = false);

        /// <summary>
        ///     Matches fields which starts with the specified value.
        /// </summary>
        TSelf WhereStartsWith(string fieldName, object value, bool exact = false);

        /// <summary>
        ///     Matches fields which starts with the specified value.
        /// </summary>
        TSelf WhereStartsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact = false);

        /// <summary>
        ///     Check if the given field exists
        /// </summary>
        /// <param name="propertySelector">Property selector for the field.</param>
        TSelf WhereExists<TValue>(Expression<Func<T, TValue>> propertySelector);

        /// <summary>
        ///     Check if the given field exists
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        TSelf WhereExists(string fieldName);

        /// <summary>
        /// Checks value of a given field against supplied regular expression pattern
        /// </summary>
        TSelf WhereRegex<TValue>(Expression<Func<T, TValue>> propertySelector, string pattern);

        /// <summary>
        /// Checks value of a given field against supplied regular expression pattern
        /// </summary>
        TSelf WhereRegex(string fieldName, string pattern);

        /// <summary>
        ///     Filter matches to be inside the specified radius
        /// </summary>
        /// <param name="propertySelector">Property selector for the field.</param>
        /// <param name="radius">Radius (measured in units passed to radiusUnits parameter) in which matches should be found.</param>
        /// <param name="latitude">Latitude pointing to a circle center.</param>
        /// <param name="longitude">Longitude pointing to a circle center.</param>
        /// <param name="radiusUnits">Units that will be used to measure distances (Kilometers, Miles).</param>
        TSelf WithinRadiusOf<TValue>(Expression<Func<T, TValue>> propertySelector, double radius, double latitude, double longitude, SpatialUnits? radiusUnits = null, double distanceErrorPct = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct);

        /// <summary>
        ///     Filter matches to be inside the specified radius
        /// </summary>
        /// <param name="fieldName">Spatial field name.</param>
        /// <param name="radius">Radius (measured in units passed to radiusUnits parameter) in which matches should be found.</param>
        /// <param name="latitude">Latitude pointing to a circle center.</param>
        /// <param name="longitude">Longitude pointing to a circle center.</param>
        /// <param name="radiusUnits">Units that will be used to measure distances (Kilometers, Miles).</param>
        TSelf WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, SpatialUnits? radiusUnits = null, double distanceErrorPct = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct);

        /// <summary>
        ///     Filter matches based on a given shape - only documents with the shape defined in fieldName that
        ///     have a relation rel with the given shapeWkt will be returned
        /// </summary>
        /// <param name="propertySelector">Property selector for the field.</param>
        /// <param name="shapeWkt">WKT formatted shape</param>
        /// <param name="relation">Spatial relation to check (Within, Contains, Disjoint, Intersects, Nearby)</param>
        /// <param name="distanceErrorPct">The allowed error percentage. By default: 0.025</param>
        TSelf RelatesToShape<TValue>(Expression<Func<T, TValue>> propertySelector, string shapeWkt, SpatialRelation relation, double distanceErrorPct = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct);

        /// <summary>
        ///     Filter matches based on a given shape - only documents with the shape defined in fieldName that
        ///     have a relation rel with the given shapeWkt will be returned
        /// </summary>
        /// <param name="propertySelector">Property selector for the field.</param>
        /// <param name="shapeWkt">WKT formatted shape</param>
        /// <param name="relation">Spatial relation to check (Within, Contains, Disjoint, Intersects, Nearby)</param>
        /// <param name="units">Units to be used</param>
        /// <param name="distanceErrorPct">The allowed error percentage. By default: 0.025</param>
        TSelf RelatesToShape<TValue>(Expression<Func<T, TValue>> propertySelector, string shapeWkt, SpatialRelation relation, SpatialUnits units, double distanceErrorPct = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct);

        /// <summary>
        ///     Filter matches based on a given shape - only documents with the shape defined in fieldName that
        ///     have a relation rel with the given shapeWkt will be returned
        /// </summary>
        /// <param name="fieldName">Spatial field name.</param>
        /// <param name="shapeWkt">WKT formatted shape</param>
        /// <param name="relation">Spatial relation to check (Within, Contains, Disjoint, Intersects, Nearby)</param>
        /// <param name="distanceErrorPct">The allowed error percentage. By default: 0.025</param>
        TSelf RelatesToShape(string fieldName, string shapeWkt, SpatialRelation relation, double distanceErrorPct = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct);

        /// <summary>
        ///     Filter matches based on a given shape - only documents with the shape defined in fieldName that
        ///     have a relation rel with the given shapeWkt will be returned
        /// </summary>
        /// <param name="fieldName">Spatial field name.</param>
        /// <param name="shapeWkt">WKT formatted shape</param>
        /// <param name="relation">Spatial relation to check (Within, Contains, Disjoint, Intersects, Nearby)</param>
        /// <param name="units">Units to be used</param>
        /// <param name="distanceErrorPct">The allowed error percentage. By default: 0.025</param>
        TSelf RelatesToShape(string fieldName, string shapeWkt, SpatialRelation relation, SpatialUnits units, double distanceErrorPct = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct);

        /// <summary>
        ///     Ability to use one factory to determine spatial shape that will be used in query.
        /// </summary>
        /// <param name="path">Spatial field name.</param>
        /// <param name="clause">function with spatial criteria factory</param>
        TSelf Spatial(Expression<Func<T, object>> path, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        /// <summary>
        ///     Ability to use one factory to determine spatial shape that will be used in query.
        /// </summary>
        /// <param name="fieldName">Spatial field name.</param>
        /// <param name="clause">function with spatial criteria factory</param>
        TSelf Spatial(string fieldName, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        TSelf Spatial(DynamicSpatialField field, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        TSelf Spatial(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        TSelf MoreLikeThis(MoreLikeThisBase moreLikeThis);
    }

    public interface IGroupByDocumentQueryBase<T, TSelf> where TSelf : IDocumentQueryBase<T, TSelf>
    {
        [Obsolete(
            @"
Use session.Query<T>() instead of session.Advanced.DocumentQuery<T>. The session.Query<T>() method fully supports Linq grouping, while session.Advanced.DocumentQuery<T>() is intended for lower level API access.
"
            , true)]
        IEnumerable<IGrouping<TKey, T>> GroupBy<TKey>(Func<T, TKey> keySelector);

        [Obsolete(
            @"
Use session.Query<T>() instead of session.Advanced.DocumentQuery<T>. The session.Query<T>() method fully supports Linq grouping, while session.Advanced.DocumentQuery<T>() is intended for lower level API access.
"
            , true)]
        IEnumerable<IGrouping<TKey, TElement>> GroupBy<TKey, TElement>(Func<T, TKey> keySelector, Func<T, TElement> elementSelector);

        [Obsolete(
            @"
Use session.Query<T>() instead of session.Advanced.DocumentQuery<T>. The session.Query<T>() method fully supports Linq grouping, while session.Advanced.DocumentQuery<T>() is intended for lower level API access.
"
            , true)]
        IEnumerable<IGrouping<TKey, T>> GroupBy<TKey>(Func<T, TKey> keySelector, IEqualityComparer<TKey> comparer);

        [Obsolete(
            @"
Use session.Query<T>() instead of session.Advanced.DocumentQuery<T>. The session.Query<T>() method fully supports Linq grouping, while session.Advanced.DocumentQuery<T>() is intended for lower level API access.
"
            , true)]
        IEnumerable<IGrouping<TKey, TElement>> GroupBy<TKey, TElement>(Func<T, TKey> keySelector, Func<T, TElement> elementSelector, IEqualityComparer<TKey> comparer);
    }

    /// <summary>
    ///     A query against a Raven index
    /// </summary>
    public interface IDocumentQueryBase<T, TSelf> : IQueryBase<T, TSelf>, IFilterDocumentQueryBase<T, TSelf>, IGroupByDocumentQueryBase<T, TSelf> where TSelf : IDocumentQueryBase<T, TSelf>
    {
        /// <summary>
        ///     Adds an ordering for a specific field to the query
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="descending">if set to <c>true</c> [descending].</param>
        /// <param name="ordering">ordering type.</param>
        TSelf AddOrder(string fieldName, bool descending = false, OrderingType ordering = OrderingType.String);

        /// <summary>
        ///     Adds an ordering for a specific field to the query
        /// </summary>
        /// <param name="propertySelector">Property selector for the field.</param>
        /// <param name="descending">if set to <c>true</c> [descending].</param>
        /// <param name="ordering">Ordering type.</param>
        TSelf AddOrder<TValue>(Expression<Func<T, TValue>> propertySelector, bool descending = false, OrderingType ordering = OrderingType.String);

        /// <summary>
        ///     Specifies a boost weight to the last where clause.
        ///     The higher the boost factor, the more relevant the term will be.
        /// </summary>
        /// <param name="boost">
        ///     boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher
        ///     weight
        /// </param>
        /// <returns></returns>
        /// <remarks>
        ///     http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
        /// </remarks>
        TSelf Boost(decimal boost);

        /// <summary>
        ///     This function exists solely to forbid in memory where clause on IDocumentQuery, because
        ///     that is nearly always a mistake.
        /// </summary>
        [Obsolete(@"
You cannot issue an in memory filter - such as Count(x=>x.Name == ""Ayende"") - on IDocumentQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.DocumentQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.DocumentQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.DocumentQuery<T>().ToList().Count(x=>x.Name == ""Ayende"")
", true)]
        int Count(Func<T, bool> predicate);

        /// <summary>
        ///     Apply distinct operation to this query
        /// </summary>
        TSelf Distinct();

        /// <summary>
        ///     Adds explanations of scores calculated for queried documents to the query result
        /// </summary>
        TSelf IncludeExplanations(out Explanations explanations);

        /// <summary>
        ///     Adds explanations of scores calculated for queried documents to the query result
        /// </summary>
        TSelf IncludeExplanations(ExplanationOptions options, out Explanations explanations);

        /// <summary>
        ///     Specifies a fuzziness factor to the single word term in the last where clause
        /// </summary>
        /// <param name="fuzzy">0.0 to 1.0 where 1.0 means closer match</param>
        /// <returns></returns>
        /// <remarks>
        ///     http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
        /// </remarks>
        TSelf Fuzzy(decimal fuzzy);

        TSelf Highlight(string fieldName, int fragmentLength, int fragmentCount, out Highlightings highlightings);

        TSelf Highlight(string fieldName, int fragmentLength, int fragmentCount, HighlightingOptions options, out Highlightings highlightings);

        TSelf Highlight(Expression<Func<T, object>> path, int fragmentLength, int fragmentCount, out Highlightings highlightings);

        TSelf Highlight(Expression<Func<T, object>> path, int fragmentLength, int fragmentCount, HighlightingOptions options, out Highlightings highlightings);

        /// <summary>
        ///     Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        TSelf Include(string path);

        /// <summary>
        ///     Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name="path">The path.</param>
        TSelf Include(Expression<Func<T, object>> path);

        /// <summary>
        ///     Partition the query so we can intersect different parts of the query
        ///     across different index entries.
        /// </summary>
        TSelf Intersect();

        TSelf OrderBy(string field, string sorterName);

        /// <summary>
        ///     Order the results by the specified fields
        ///     The field is the name of the field to sort, defaulting to sorting by ascending.
        /// </summary>
        TSelf OrderBy(string field, OrderingType ordering = OrderingType.String);

        /// <summary>
        ///     Order the results by the specified fields
        ///     The field is the name of the field to sort, defaulting to sorting by ascending.
        /// </summary>
        TSelf OrderBy<TValue>(Expression<Func<T, TValue>> propertySelector);

        TSelf OrderBy<TValue>(Expression<Func<T, TValue>> propertySelector, string sorterName);

        /// <summary>
        ///     Order the results by the specified fields
        ///     The field is the name of the field to sort, defaulting to sorting by ascending.
        /// </summary>
        TSelf OrderBy<TValue>(Expression<Func<T, TValue>> propertySelector, OrderingType ordering);

        /// <summary>
        ///     Order the results by the specified fields
        ///     The field is the name of the field to sort, defaulting to sorting by ascending.
        /// </summary>
        /// <param name="propertySelectors">Property selectors for the fields.</param>
        TSelf OrderBy<TValue>(params Expression<Func<T, TValue>>[] propertySelectors);

        TSelf OrderByDescending(string field, string sorterName);

        /// <summary>
        ///     Order the results by the specified fields
        ///     The field is the name of the field to sort, defaulting to sorting by descending.
        /// </summary>
        TSelf OrderByDescending(string field, OrderingType ordering = OrderingType.String);

        /// <summary>
        ///     Order the results by the specified fields
        ///     The field is the name of the field to sort, defaulting to sorting by ascending.
        /// </summary>
        TSelf OrderByDescending<TValue>(Expression<Func<T, TValue>> propertySelector);

        TSelf OrderByDescending<TValue>(Expression<Func<T, TValue>> propertySelector, string sorterName);

        /// <summary>
        ///     Order the results by the specified fields
        ///     The field is the name of the field to sort, defaulting to sorting by ascending.
        /// </summary>
        TSelf OrderByDescending<TValue>(Expression<Func<T, TValue>> propertySelector, OrderingType ordering);

        /// <summary>
        ///     Order the results by the specified fields
        ///     The field is the name of the field to sort, defaulting to sorting by descending.
        /// </summary>
        /// <param name="propertySelectors">Property selectors for the fields.</param>
        TSelf OrderByDescending<TValue>(params Expression<Func<T, TValue>>[] propertySelectors);

        /// <summary>
        ///     Adds an ordering by score for a specific field to the query
        /// </summary>
        TSelf OrderByScore();

        /// <summary>
        ///     Adds an ordering by score for a specific field to the query
        /// </summary>
        TSelf OrderByScoreDescending();

        /// <summary>
        ///     Specifies a proximity distance for the phrase in the last where clause
        /// </summary>
        /// <param name="proximity">number of words within</param>
        /// <returns></returns>
        /// <remarks>
        ///     http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
        /// </remarks>
        TSelf Proximity(int proximity);

        /// <summary>
        ///     Order the search results randomly
        /// </summary>
        TSelf RandomOrdering();

        /// <summary>
        ///     Order the search results randomly using the specified seed
        ///     this is useful if you want to have repeatable random queries
        /// </summary>
        TSelf RandomOrdering(string seed);

#if FEATURE_CUSTOM_SORTING
        /// <summary>
        /// Order the search results randomly
        /// </summary>
        TSelf CustomSortUsing(string typeName, bool descending);
#endif

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistance(DynamicSpatialField field, double latitude, double longitude);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistance(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, double latitude, double longitude);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistance(DynamicSpatialField field, string shapeWkt);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistance(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, string shapeWkt);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistance(Expression<Func<T, object>> propertySelector, double latitude, double longitude);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistance(string fieldName, double latitude, double longitude);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistance(Expression<Func<T, object>> propertySelector, string shapeWkt);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistance(string fieldName, string shapeWkt);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistanceDescending(DynamicSpatialField field, double latitude, double longitude);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistanceDescending(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, double latitude, double longitude);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistanceDescending(DynamicSpatialField field, string shapeWkt);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistanceDescending(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, string shapeWkt);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistanceDescending(Expression<Func<T, object>> propertySelector, double latitude, double longitude);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistanceDescending(string fieldName, double latitude, double longitude);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistanceDescending(Expression<Func<T, object>> propertySelector, string shapeWkt);

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        TSelf OrderByDistanceDescending(string fieldName, string shapeWkt);
    }
}
