using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Queries.Spatial;
using Raven.Client.Documents.Session.Loaders;

namespace Raven.Client.Documents.Session
{
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
        ///     Wraps previous query with clauses and add an AND operation to the given query
        /// </summary>
        TSelf AndAlso(bool wrapPreviousQueryClauses);

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
        ///     Performs a query matching ALL of the provided values against the given field (AND)
        /// </summary>
        TSelf ContainsAll<TValue>(Expression<Func<T, IEnumerable<TValue>>> propertySelector, IEnumerable<TValue> values);

        /// <summary>
        ///     Performs a query matching ANY of the provided values against the given field (OR)
        /// </summary>
        TSelf ContainsAny(string fieldName, IEnumerable<object> values);

        /// <summary>
        ///     Performs a query matching ANY of the provided values against the given field (OR)
        /// </summary>
        TSelf ContainsAny<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values);

        /// <summary>
        ///     Performs a query matching ANY of the provided values against the given field (OR)
        /// </summary>
        TSelf ContainsAny<TValue>(Expression<Func<T, IEnumerable<TValue>>> propertySelector, IEnumerable<TValue> values);

        /// <summary>
        ///     Negate the next operation
        /// </summary>
        TSelf NegateNext();

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
        TSelf WhereEndsWith(string fieldName, object value);

        /// <summary>
        ///     Matches fields which ends with the specified value.
        /// </summary>
        TSelf WhereEndsWith(string fieldName, object value, bool exact);

        /// <summary>
        ///     Matches fields which ends with the specified value.
        /// </summary>
        TSelf WhereEndsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value);

        /// <summary>
        ///     Matches fields which ends with the specified value.
        /// </summary>
        TSelf WhereEndsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact);

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
        TSelf WhereStartsWith(string fieldName, object value);

        /// <summary>
        ///     Matches fields which starts with the specified value.
        /// </summary>
        TSelf WhereStartsWith(string fieldName, object value, bool exact);

        /// <summary>
        ///     Matches fields which starts with the specified value.
        /// </summary>
        TSelf WhereStartsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value);

        /// <summary>
        ///     Matches fields which starts with the specified value.
        /// </summary>
        TSelf WhereStartsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact);

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

        /// <inheritdoc cref="MoreLikeThisBase"/>
        /// <param name="moreLikeThis">Specify MoreLikeThisQuery.</param>
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
    ///     Interface providing low-level querying capabilities.
    /// </summary>
    /// <inheritdoc cref="DocumentationUrls.Session.Querying.QueryVsDocumentQuery"/>
    public interface IDocumentQueryBase<T, TSelf> : IPagingDocumentQueryBase<T, TSelf>, IFilterDocumentQueryBase<T, TSelf>, IGroupByDocumentQueryBase<T, TSelf>, IQueryBase<T, TSelf>
        where TSelf : IDocumentQueryBase<T, TSelf>
    {
        /// <summary>
        ///     Orders query results by specified field.
        /// </summary>
        /// <param name="fieldName">Name of the field to order the query results by.</param>
        /// <param name="descending">Specifies if order is descending. Default: false.</param>
        /// <param name="ordering">Ordering type. Default: OrderingType.String.</param>
        TSelf AddOrder(string fieldName, bool descending = false, OrderingType ordering = OrderingType.String);

        /// <summary>
        ///     Orders query results by specified field.
        /// </summary>
        /// <param name="propertySelector">Path to the field to order the query results by.</param>
        /// <param name="descending">Specifies if order is descending. Default: false.</param>
        /// <param name="ordering">Ordering type. Default: OrderingType.String.</param>
        TSelf AddOrder<TValue>(Expression<Func<T, TValue>> propertySelector, bool descending = false, OrderingType ordering = OrderingType.String);

        /// <summary>
        ///     Specifies boost weight for the preceding Where clause.
        ///     The higher the boost weight, the more relevant the term will be.
        ///     By default all terms have weight of 1.0.
        /// </summary>
        /// <param name="boost">Boost weight.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.BoostSearchResults"/>
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
        ///     Removes duplicates from query results.
        /// </summary>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.Distinct"/>
        TSelf Distinct();

        /// <inheritdoc cref="AbstractDocumentQuery{T, TSelf}.IncludeExplanations"/>
        TSelf IncludeExplanations(out Explanations explanations);

        /// <inheritdoc cref="AbstractDocumentQuery{T, TSelf}.IncludeExplanations"/>
        TSelf IncludeExplanations(ExplanationOptions options, out Explanations explanations);

        /// <summary>
        ///     Specifies a fuzziness factor for the preceding WhereEquals clause,
        ///     making it match documents containing terms similar to searched one.
        ///     The higher the factor, the more similar terms will be matched.
        /// </summary>
        /// <param name="fuzzy">Decimal value between 0.0 and 1.0.</param>
        /// <returns></returns>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.FuzzySearch"/>
        TSelf Fuzzy(decimal fuzzy);

        /// <inheritdoc cref="Highlight(string, int, int, HighlightingOptions, out Highlightings)"/>
        TSelf Highlight(string fieldName, int fragmentLength, int fragmentCount, out Highlightings highlightings);

        /// <inheritdoc cref="Linq.IRavenQueryable{T}.Highlight(string, int, int, HighlightingOptions, out Highlightings)"/>
        TSelf Highlight(string fieldName, int fragmentLength, int fragmentCount, HighlightingOptions options, out Highlightings highlightings);

        /// <inheritdoc cref="Highlight(Expression{Func{T, object}}, int, int, HighlightingOptions, out Highlightings)"/>
        TSelf Highlight(Expression<Func<T, object>> path, int fragmentLength, int fragmentCount, out Highlightings highlightings);

        /// <inheritdoc cref="Linq.IRavenQueryable{T}.Highlight(Expression{Func{T, object}}, int, int, HighlightingOptions, out Highlightings)"/>
        TSelf Highlight(Expression<Func<T, object>> path, int fragmentLength, int fragmentCount, HighlightingOptions options, out Highlightings highlightings);

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(string)"/>
        TSelf Include(string path);

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(Expression{Func{T, string}})"/>
        TSelf Include(Expression<Func<T, object>> path);

        /// <summary>
        ///     Gets the intersection of sub-queries - documents that match all provided sub-queries.
        /// </summary>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToUseIntersect"/>
        TSelf Intersect();

        /// <summary>
        ///     Orders the query results using server-side custom sorter by the specified field in ascending order.
        /// </summary>
        /// <param name="field">Name of the field to order the query results by.</param>
        /// <param name="sorterName">Name of the custom sorter to be used.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.SortQueryResults.OrderByFieldValue"/>
        TSelf OrderBy(string field, string sorterName);

        /// <summary>
        ///     Orders the query results by the specified field in ascending order.
        /// </summary>
        /// <param name="field">Name of the field to order the query results by.</param>
        /// <param name="ordering">Ordering type. Default: OrderingType.String.</param>
        TSelf OrderBy(string field, OrderingType ordering = OrderingType.String);

        /// <inheritdoc cref="OrderBy{TValue}(Expression{Func{T, TValue}}, OrderingType)"/>
        TSelf OrderBy<TValue>(Expression<Func<T, TValue>> propertySelector);

        /// <summary>
        ///     Orders the query results using custom server-side sorter by the specified field in ascending order.
        /// </summary>
        /// <param name="propertySelector">Path to the field to order the query results by.</param>
        /// <param name="sorterName">Name of the custom sorter to be used.</param>
        TSelf OrderBy<TValue>(Expression<Func<T, TValue>> propertySelector, string sorterName);

        /// <summary>
        ///     Orders the query results by the specified field in ascending order.
        /// </summary>
        /// <param name="propertySelector">Path to the field to order the query results by.</param>
        /// <param name="ordering">Ordering type. Default: OrderingType.String.</param>
        TSelf OrderBy<TValue>(Expression<Func<T, TValue>> propertySelector, OrderingType ordering);

        /// <summary>
        ///     Orders the query results by specified fields in ascending order.
        /// </summary>
        /// <param name="propertySelectors">List of field paths to order the query results by.</param>
        TSelf OrderBy<TValue>(params Expression<Func<T, TValue>>[] propertySelectors);

        /// <summary>
        ///     Orders the query results using server-side custom sorter by the specified field in descending order.
        /// </summary>
        /// <param name="field">Name of field to order the results by.</param>
        /// <param name="sorterName">Name of the custom sorter to be used.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.SortQueryResults.OrderByFieldValue"/>
        TSelf OrderByDescending(string field, string sorterName);

        /// <summary>
        ///     Orders the query results by the specified field in descending order.
        /// </summary>
        /// <param name="field">Name of the field to order the query results by.</param>
        /// <param name="ordering">Ordering type. Default: OrderingType.String.</param>
        TSelf OrderByDescending(string field, OrderingType ordering = OrderingType.String);

        /// <inheritdoc cref="OrderByDescending{TValue}(Expression{Func{T, TValue}}, OrderingType)"/>
        TSelf OrderByDescending<TValue>(Expression<Func<T, TValue>> propertySelector);

        /// <summary>
        ///     Orders the query results using server-side custom sorter by the specified field in descending order.
        /// </summary>
        /// <param name="propertySelector">Path to the field to order the query results by.</param>
        /// <param name="sorterName">Name of the custom sorter to be used.</param>
        TSelf OrderByDescending<TValue>(Expression<Func<T, TValue>> propertySelector, string sorterName);

        /// <summary>
        ///     Orders the query results by the specified field in descending order.
        /// </summary>
        /// <param name="propertySelector">Path to the field to order the query results by.</param>
        /// <param name="ordering">Ordering type. Default: OrderingType.String.</param>
        TSelf OrderByDescending<TValue>(Expression<Func<T, TValue>> propertySelector, OrderingType ordering);

        /// <summary>
        ///     Orders the query results by specified fields in descending order.
        /// </summary>
        /// <param name="propertySelectors">List of field paths to order the query results by.</param>
        TSelf OrderByDescending<TValue>(params Expression<Func<T, TValue>>[] propertySelectors);

        /// <summary>
        ///     Sorts query results by score. Results with higher score will be returned first.
        /// </summary>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.SortQueryResults.OrderByScore"/>
        TSelf OrderByScore();

        /// <summary>
        ///     Sorts query results by score. Results with lower score will be returned first.
        /// </summary>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.SortQueryResults.OrderByScore"/>
        TSelf OrderByScoreDescending();

        /// <summary>
        ///     Specifies a proximity distance between terms in the preceding Search clause,
        ///     making it return documents that contain searched terms separated by at
        ///     most that many other terms.
        /// </summary>
        /// <param name="proximity">Maximum number of different terms separating searched terms.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.ProximitySearch"/>
        TSelf Proximity(int proximity);

        /// <summary>
        ///     Orders the query results randomly.
        /// </summary>
        TSelf RandomOrdering();

        /// <summary>
        ///     Orders the query results randomly using the specified seed.
        ///     Allows to have repeatable random query results.
        /// </summary>
        /// <param name="seed">Seed to be used for pseudorandom number generator.</param>
        TSelf RandomOrdering(string seed);

#if FEATURE_CUSTOM_SORTING
        /// <summary>
        ///     Sorts query results using server-side custom sorter.
        ///     Requires custom sorting feature to be enabled.
        /// </summary>
        /// <param name="typeName">Name of the custom sorter to be used.</param>
        /// <param name="descending">Sets the order to descending.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.SortQueryResults.CustomSorters"/>
        TSelf CustomSortUsing(string typeName, bool descending);
#endif

        /// <summary>
        ///     Sorts the spatial query results by distance from a given geographical coordinates in ascending order.
        /// </summary>
        /// <param name="field">Spatial field used for distance calculation.</param>
        /// <param name="latitude">Latitude of coordinates to calculate the distance from.</param>
        /// <param name="longitude">Longitude of coordinates to calculate the distance from.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistance(DynamicSpatialField field, double latitude, double longitude);

        /// <summary>
        ///     Sorts the spatial query results by distance from a given geographical coordinates in ascending order.
        /// </summary>
        /// <param name="field">Spatial field used for distance calculation.</param>
        /// <param name="latitude">Latitude of coordinates to calculate the distance from.</param>
        /// <param name="longitude">Longitude of coordinates to calculate the distance from.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistance(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, double latitude, double longitude);

        /// <summary>
        ///     Sorts the spatial query results by distance from the center of given WKT shape in ascending order.
        /// </summary>
        /// <param name="field">Spatial field used for distance calculation.</param>
        /// <param name="shapeWkt">String representing the WKT shape to calculate the distance from.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistance(DynamicSpatialField field, string shapeWkt);

        /// <summary>
        ///     Sorts the spatial query results by distance from the center of given WKT shape in ascending order.
        /// </summary>
        /// <param name="field">Spatial field used for distance calculation.</param>
        /// <param name="shapeWkt">String representing the WKT shape to calculate the distance from.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistance(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, string shapeWkt);

        /// <inheritdoc cref="OrderByDistance(Expression{Func{T, object}}, double, double, double)"/>
        TSelf OrderByDistance(Expression<Func<T, object>> propertySelector, double latitude, double longitude);

        /// <summary>
        ///     Sorts the spatial query results by distance from given geographical coordinates in ascending order.
        /// </summary>
        /// <param name="propertySelector">Path to the spatial field used for distance calculation.</param>
        /// <param name="latitude">Latitude of coordinates to calculate the distance from.</param>
        /// <param name="longitude">Longitude of coordinates to calculate the distance from.</param>
        /// <param name="roundFactor">Distance interval in kilometers. The distance from coordinates is rounded up to the nearest interval.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistance(Expression<Func<T, object>> propertySelector, double latitude, double longitude, double roundFactor);

        /// <inheritdoc cref="OrderByDistance(string, double, double, double)"/>
        TSelf OrderByDistance(string fieldName, double latitude, double longitude);

        /// <summary>
        ///     Sorts the spatial query results by distance from given geographical coordinates in ascending order.
        /// </summary>
        /// <param name="fieldName">Name of the spatial field used for distance calculation.</param>
        /// <param name="latitude">Latitude of coordinates to calculate the distance from.</param>
        /// <param name="longitude">Longitude of coordinates to calculate the distance from.</param>
        /// <param name="roundFactor">Distance interval in kilometers. The distance from coordinates is rounded up to the nearest interval.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistance(string fieldName, double latitude, double longitude, double roundFactor);

        /// <inheritdoc cref="OrderByDistance(Expression{Func{T, object}}, string, double)"/>
        TSelf OrderByDistance(Expression<Func<T, object>> propertySelector, string shapeWkt);

        /// <summary>
        ///     Sorts the spatial query results by distance from the center of given WKT shape in ascending order.
        /// </summary>
        /// <param name="propertySelector">Path to the spatial field used for distance calculation.</param>
        /// <param name="shapeWkt">String representing the WKT shape to calculate the distance from.</param>
        /// <param name="roundFactor">Distance interval in kilometers. The distance from WKT shape is rounded up to the nearest interval.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistance(Expression<Func<T, object>> propertySelector, string shapeWkt, double roundFactor);

        /// <inheritdoc cref="OrderByDistance(string, string, double)"/>
        TSelf OrderByDistance(string fieldName, string shapeWkt);

        /// <summary>
        ///     Sorts the spatial query results by distance from the center of given WKT shape in ascending order.
        /// </summary>
        /// <param name="fieldName">Name of the spatial field used for distance calculation.</param>
        /// <param name="shapeWkt">String representing the WKT shape to calculate the distance from.</param>
        /// <param name="roundFactor">Distance interval in kilometers. The distance from WKT shape is rounded up to the nearest interval.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistance(string fieldName, string shapeWkt, double roundFactor);

        /// <summary>
        ///     Sorts the spatial query results by distance from given geographical coordinates in descending order.
        /// </summary>
        /// <param name="field">Spatial field used for distance calculation.</param>
        /// <param name="latitude">Latitude of coordinates to calculate the distance from.</param>
        /// <param name="longitude">Longitude of coordinates to calculate the distance from.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistanceDescending(DynamicSpatialField field, double latitude, double longitude);

        /// <summary>
        ///     Sorts the spatial query results by distance from given geographical coordinates in descending order.
        /// </summary>
        /// <param name="field">Spatial field used for distance calculation.</param>
        /// <param name="latitude">Latitude of coordinates to calculate the distance from.</param>
        /// <param name="longitude">Longitude of coordinates to calculate the distance from.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistanceDescending(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, double latitude, double longitude);

        /// <inheritdoc cref="OrderByDistanceDescending(Func{DynamicSpatialFieldFactory{T}, DynamicSpatialField}, string)"/>
        TSelf OrderByDistanceDescending(DynamicSpatialField field, string shapeWkt);

        /// <summary>
        ///     Sorts the spatial query results by distance from the center of given WKT shape in descending order.
        /// </summary>
        /// <param name="field">Spatial field used for distance calculation.</param>
        /// <param name="shapeWkt">String representing the WKT shape to calculate the distance from.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistanceDescending(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, string shapeWkt);

        /// <inheritdoc cref="OrderByDistanceDescending(Expression{Func{T, object}}, double, double, double)"/>
        TSelf OrderByDistanceDescending(Expression<Func<T, object>> propertySelector, double latitude, double longitude);

        /// <summary>
        ///     Sorts the spatial query results by distance from a given geographical coordinates in descending order.
        /// </summary>
        /// <param name="propertySelector">Path to the spatial field used for distance calculation.</param>
        /// <param name="latitude">Latitude of coordinates to calculate the distance from.</param>
        /// <param name="longitude">Longitude of coordinates to calculate the distance from.</param>
        /// <param name="roundFactor">Distance interval in kilometers. The distance from coordinates is rounded up to the nearest interval.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistanceDescending(Expression<Func<T, object>> propertySelector, double latitude, double longitude, double roundFactor);

        /// <inheritdoc cref="OrderByDistanceDescending(string, double, double, double)"></inheritdoc>
        TSelf OrderByDistanceDescending(string fieldName, double latitude, double longitude);

        /// <summary>
        ///     Sorts the spatial query results by distance from given geographical coordinates in descending order.
        /// </summary>
        /// <param name="fieldName">Name of spatial field used for distance calculation.</param>
        /// <param name="latitude">Latitude of coordinates to calculate the distance from.</param>
        /// <param name="longitude">Longitude of coordinates to calculate the distance from.</param>
        /// <param name="roundFactor">Distance interval in kilometers. The distance from coordinates is rounded up to the nearest interval.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistanceDescending(string fieldName, double latitude, double longitude, double roundFactor);

        /// <inheritdoc cref="OrderByDistanceDescending(Expression{Func{T, object}}, string, double)"/>
        TSelf OrderByDistanceDescending(Expression<Func<T, object>> propertySelector, string shapeWkt);

        /// <summary>
        ///     Sorts the spatial query results by distance from the center of given WKT shape in descending order.
        /// </summary>
        /// <param name="propertySelector">Property selector for the spatial field used for distance calculation.</param>
        /// <param name="shapeWkt">String representing the WKT shape to calculate the distance from.</param>
        /// <param name="roundFactor">Distance interval in kilometers. The distance from WKT shape is rounded up to the nearest interval.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistanceDescending(Expression<Func<T, object>> propertySelector, string shapeWkt, double roundFactor);

        /// <inheritdoc cref="OrderByDistanceDescending(string, string, double)"/>
        TSelf OrderByDistanceDescending(string fieldName, string shapeWkt);

        /// <summary>
        ///     Sorts the spatial query results by distance from the center of given WKT shape in descending order.
        /// </summary>
        /// <param name="fieldName">Name of spatial field used for distance calculation.</param>
        /// <param name="shapeWkt">String representing the WKT shape to calculate the distance from.</param>
        /// <param name="roundFactor">Distance interval in kilometers. The distance from WKT shape is rounded up to the nearest interval.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToMakeASpatialQuery"/>
        TSelf OrderByDistanceDescending(string fieldName, string shapeWkt, double roundFactor);
    }
}
