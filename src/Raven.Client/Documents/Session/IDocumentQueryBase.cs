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
        ///     Negates the next subclause.
        /// </summary>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToUseNotOperator"/>
        TSelf Not { get; }

        /// <inheritdoc cref="AndAlso(bool)"/>
        TSelf AndAlso();

        /// <summary>
        ///     Adds an 'AND' statement to the query.
        /// </summary>
        /// <param name="wrapPreviousQueryClauses">Wraps preceding clauses using parentheses.</param>
        TSelf AndAlso(bool wrapPreviousQueryClauses);

        /// <summary>
        ///     Closes previously opened subclause.
        /// </summary>
        TSelf CloseSubclause();

        /// <summary>
        ///     Matches documents with chosen field containing all provided values.
        /// </summary>
        /// <param name="fieldName">Name of the field to match values against.</param>
        /// <param name="values">Values that the chosen field has to contain.</param>
        TSelf ContainsAll(string fieldName, IEnumerable<object> values);

        /// <inheritdoc cref="ContainsAll{TValue}(Expression{Func{T, IEnumerable{TValue}}}, IEnumerable{TValue})"/>
        TSelf ContainsAll<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values);

        /// <summary>
        ///     Matches documents with chosen field containing all provided values.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to match values against.</param>
        /// <param name="values">Values that the chosen field has to contain.</param>
        TSelf ContainsAll<TValue>(Expression<Func<T, IEnumerable<TValue>>> propertySelector, IEnumerable<TValue> values);

        /// <summary>
        ///     Matches documents with chosen field containing any of provided values.
        /// </summary>
        /// <param name="fieldName">Name of the field to match values against.</param>
        /// <param name="values">Values, where at least one must be contained in <paramref name="fieldName"/> value in order to match the document.</param>
        TSelf ContainsAny(string fieldName, IEnumerable<object> values);

        /// <inheritdoc cref="ContainsAny{TValue}(Expression{Func{T, IEnumerable{TValue}}}, IEnumerable{TValue})"/>
        TSelf ContainsAny<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values);

        /// <summary>
        ///     Matches documents with chosen field containing any of provided values.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to match values against.</param>
        /// <param name="values">Values, where at least one must be contained in <paramref name="propertySelector"/> value in order to match the document.</param>
        TSelf ContainsAny<TValue>(Expression<Func<T, IEnumerable<TValue>>> propertySelector, IEnumerable<TValue> values);

        /// <summary>
        ///     Negates the next subclause.
        /// </summary>
        TSelf NegateNext();

        /// <summary>
        ///     Opens a new subclause.
        /// </summary>
        TSelf OpenSubclause();

        /// <summary>
        ///     Adds an 'OR' statement to the query.
        /// </summary>
        TSelf OrElse();

        /// <summary>
        ///     Matches documents with value of chosen field matching searched terms.
        /// </summary>
        /// <param name="fieldName">Name of the field that searched terms will be checked against.</param>
        /// <param name="searchTerms">Space separated terms to search. If there is more than a single term, each of them will be checked independently.</param>
        /// <param name="operator">Operator to be used for relationship between terms. Default: Or.</param>
        TSelf Search(string fieldName, string searchTerms, SearchOperator @operator = SearchOperator.Or);

        /// <summary>
        ///     Matches documents with value of chosen field matching searched terms.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field that searched terms will be checked against.</param>
        /// <param name="searchTerms">Space separated terms to search. If there is more than a single term, each of them will be checked independently.</param>
        /// <param name="operator">Operator to be used for relationship between terms. Default: Or.</param>
        TSelf Search<TValue>(Expression<Func<T, TValue>> propertySelector, string searchTerms, SearchOperator @operator = SearchOperator.Or);

        /// <summary>
        ///     This function exists solely to forbid in memory where clause on IDocumentQuery, because
        ///     that is nearly always a mistake.
        /// </summary>
        IEnumerable<T> Where(Func<T, bool> predicate);

        /// <inheritdoc cref="WhereLucene(string, string, bool)"/>
        TSelf WhereLucene(string fieldName, string whereClause);

        /// <summary>
        ///     Matches documents with chosen field value meeting criteria of specified predicate in Lucene syntax.
        /// </summary>
        /// <param name="fieldName">Name of the field to get value from.</param>
        /// <param name="whereClause">Predicate in Lucene syntax.</param>
        /// <param name="exact">Specifies if comparison is case sensitive.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToUseLucene"/>
        TSelf WhereLucene(string fieldName, string whereClause, bool exact);

        /// <summary>
        ///     Matches documents with value of the chosen field between the specified start and end value, inclusive. 
        /// </summary>
        /// <param name="fieldName">Name of the field to get value from.</param>
        /// <param name="start">Start value.</param>
        /// <param name="end">End value.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereBetween(string fieldName, object start, object end, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field between specified the start and end, inclusive.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to get value from.</param>
        /// <param name="start">Start value.</param>
        /// <param name="end">End value.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereBetween<TValue>(Expression<Func<T, TValue>> propertySelector, TValue start, TValue end, bool exact = false);

        /// <inheritdoc cref="WhereEndsWith(string, object, bool)"/>
        TSelf WhereEndsWith(string fieldName, object value);

        /// <summary>
        ///     Matches documents with value of the chosen field ending with the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field to get value from.</param>
        /// <param name="value">Value that the <paramref name="fieldName"/> value has to end with in order to match the document.</param>
        /// <param name="exact">Specifies if comparison is case sensitive.</param>
        TSelf WhereEndsWith(string fieldName, object value, bool exact);

        /// <inheritdoc cref="WhereEndsWith{TValue}(Expression{Func{T, TValue}}, TValue, bool)"/>
        TSelf WhereEndsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value);

        /// <summary>
        ///     Matches documents with value of the chosen field ending with the specified value.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to get value from.</param>
        /// <param name="value">Value that the <paramref name="propertySelector"/> value has to end with in order to match the document.</param>
        /// <param name="exact">Specifies if comparison is case sensitive.</param>
        TSelf WhereEndsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact);

        /// <summary>
        ///     Matches documents with value of the chosen field equal to the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field to get value from.</param>
        /// <param name="value">Value to compare with <paramref name="fieldName"/> value.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereEquals(string fieldName, object value, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field equal to the evaluated provided expression.
        /// </summary>
        /// <param name="fieldName">Name of the field to get value from.</param>
        /// <param name="value">Expression to evaluate.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereEquals(string fieldName, MethodCall value, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field equal to the specified value.
        /// </summary>
        /// <param name="propertySelector">Name of the field to get value from.</param>
        /// <param name="value"></param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field equal to the evaluated provided expression.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to get value from.</param>
        /// <param name="value">Expression to evaluate.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, MethodCall value, bool exact = false);

        /// <summary>
        ///     Matches documents that match specified <paramref name="whereParams"/>.
        /// </summary>
        /// <param name="whereParams">WhereParams containing query parameters.</param>
        TSelf WhereEquals(WhereParams whereParams);

        /// <summary>
        ///     Matches documents with value of the chosen field different than the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field to get value from.</param>
        /// <param name="value">Value to compare with <paramref name="fieldName"/> value.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereNotEquals(string fieldName, object value, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field different than the evaluated provided expression.
        /// </summary>
        /// <param name="fieldName">Name of the field to get value from.</param>
        /// <param name="value">Expression to evaluate.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereNotEquals(string fieldName, MethodCall value, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field different than the specified value.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to get value from.</param>
        /// <param name="value">Value to compare with <paramref name="propertySelector"/> value.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereNotEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field different than the evaluated provided expression.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to get value from.</param>
        /// <param name="value">Expression to evaluate.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereNotEquals<TValue>(Expression<Func<T, TValue>> propertySelector, MethodCall value, bool exact = false);

        /// <summary>
        ///     Matches documents that do not match specified <paramref name="whereParams"/>.
        /// </summary>
        /// <param name="whereParams">WhereParams containing query parameters.</param>
        TSelf WhereNotEquals(WhereParams whereParams);

        /// <summary>
        ///     Matches documents with value of the chosen field greater than the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field to get value from.</param>
        /// <param name="value">Value to compare with <paramref name="fieldName"/> value.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereGreaterThan(string fieldName, object value, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field greater than the specified value.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to get value from.</param>
        /// <param name="value">Value to compare with <paramref name="propertySelector"/> value.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereGreaterThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field greater than or equal to the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field to get value from.</param>
        /// <param name="value">Value to compare with <paramref name="fieldName"/> value.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereGreaterThanOrEqual(string fieldName, object value, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field greater than or equal to the specified value.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to get value from.</param>
        /// <param name="value">Value to compare with <paramref name="propertySelector"/> value.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereGreaterThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field contained in provided values.
        /// </summary>
        /// <param name="fieldName">Name of the field to get value from.</param>
        /// <param name="values">Values that have to contain <paramref name="fieldName"/> value in order for the document to be matched.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereIn(string fieldName, IEnumerable<object> values, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field contained in provided values.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to get value from.</param>
        /// <param name="values">Values that have to contain <paramref name="propertySelector"/> value in order for the document to be matched.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereIn<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field less than the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field to get value from.</param>
        /// <param name="value">Value to compare with <paramref name="fieldName"/> value.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereLessThan(string fieldName, object value, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field less than the specified value.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to get value from.</param>
        /// <param name="value">Value to compare with <paramref name="propertySelector"/> value.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereLessThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field less than or equal to the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field to get value from.</param>
        /// <param name="value">Value to compare with <paramref name="fieldName"/> value.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereLessThanOrEqual(string fieldName, object value, bool exact = false);

        /// <summary>
        ///     Matches documents with value of the chosen field less than or equal to the specified value.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to get value from.</param>
        /// <param name="value">Value to compare with <paramref name="propertySelector"/> value.</param>
        /// <param name="exact">Specifies if comparison is case sensitive. Default: false.</param>
        TSelf WhereLessThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact = false);

        /// <inheritdoc cref="WhereStartsWith(string, object, bool)"/>
        TSelf WhereStartsWith(string fieldName, object value);

        /// <summary>
        ///     Matches documents with value of the chosen field starting with the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field to get value from.</param>
        /// <param name="value">Value that the <paramref name="fieldName"/> value has to start with in order to match the document.</param>
        /// <param name="exact">Specifies if comparison is case sensitive.</param>
        TSelf WhereStartsWith(string fieldName, object value, bool exact);

        /// <inheritdoc cref="WhereStartsWith{TValue}(Expression{Func{T, TValue}}, TValue, bool)"/>
        TSelf WhereStartsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value);

        /// <summary>
        ///     Matches documents with value of the chosen field starting with the specified value.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to get value from.</param>
        /// <param name="value">Value that the <paramref name="propertySelector"/> value has to start with in order to match the document.</param>
        /// <param name="exact">Specifies if comparison is case sensitive.</param>
        TSelf WhereStartsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool exact);

        /// <summary>
        ///     Matches documents with existing given field.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to check the existence of.</param>
        TSelf WhereExists<TValue>(Expression<Func<T, TValue>> propertySelector);

        /// <summary>
        ///     Matches documents with existing given field.
        /// </summary>
        /// <param name="fieldName">Name of the field to check the existence of.</param>
        TSelf WhereExists(string fieldName);

        /// <summary>
        ///     Matches documents with the value of a given field matched by provided regular expression.
        /// </summary>
        /// <param name="propertySelector">Property selector for the field to get value from.</param>
        /// <param name="pattern">Regular expression pattern to check field value against.</param>
        TSelf WhereRegex<TValue>(Expression<Func<T, TValue>> propertySelector, string pattern);

        /// <summary>
        ///     Matches documents with the value of a given field matched by provided regular expression.
        /// </summary>
        /// <param name="fieldName">Name of the field to get value from.</param>
        /// <param name="pattern">Regular expression pattern to check field value against.</param>
        TSelf WhereRegex(string fieldName, string pattern);

        /// <summary>
        ///     Matches documents with the value of specified field in radius of given spatial circle.
        /// </summary>
        /// <param name="propertySelector">Property selector for the spatial field to get the value from.</param>
        /// <param name="radius">Radius (measured in units passed to radiusUnits parameter) in which matches should be found.</param>
        /// <param name="latitude">Latitude of a circle center.</param>
        /// <param name="longitude">Longitude of a circle center.</param>
        /// <param name="radiusUnits">Units that the radius was measured in (kilometers or miles).</param>
        /// <param name="distanceErrorPct">Allowed error percentage. Default: 0.025.</param>
        TSelf WithinRadiusOf<TValue>(Expression<Func<T, TValue>> propertySelector, double radius, double latitude, double longitude, SpatialUnits? radiusUnits = null, double distanceErrorPct = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct);

        /// <summary>
        ///     Matches documents with the value of specified field in radius of given spatial circle.
        /// </summary>
        /// <param name="fieldName">Name of the spatial field to get the value from.</param>
        /// <param name="radius">Radius (measured in units passed to radiusUnits parameter) in which matches should be found.</param>
        /// <param name="latitude">Latitude of a circle center.</param>
        /// <param name="longitude">Longitude of a circle center.</param>
        /// <param name="radiusUnits">Units that the radius was measured in (kilometers or miles).</param>
        /// /// <param name="distanceErrorPct">Allowed error percentage. Default: 0.025.</param>
        TSelf WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, SpatialUnits? radiusUnits = null, double distanceErrorPct = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct);

        /// <inheritdoc cref="RelatesToShape{TValue}(Expression{Func{T, TValue}}, string, SpatialRelation, SpatialUnits, double)"/>
        TSelf RelatesToShape<TValue>(Expression<Func<T, TValue>> propertySelector, string shapeWkt, SpatialRelation relation, double distanceErrorPct = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct);

        /// <summary>
        ///     Matches documents with the value of specified field in relation with the provided WKT shape.
        /// </summary>
        /// <param name="propertySelector">Property selector for the spatial field to get the value from.</param>
        /// <param name="shapeWkt">String representing the WKT shape.</param>
        /// <param name="relation">Spatial relation to check (Within, Contains, Disjoint, Intersects).</param>
        /// <param name="units">Units to be used (kilometers or miles).</param>
        /// <param name="distanceErrorPct">Allowed error percentage. Default: 0.025.</param>
        TSelf RelatesToShape<TValue>(Expression<Func<T, TValue>> propertySelector, string shapeWkt, SpatialRelation relation, SpatialUnits units, double distanceErrorPct = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct);

        /// <inheritdoc cref="RelatesToShape(string, string, SpatialRelation, SpatialUnits, double)"/>
        TSelf RelatesToShape(string fieldName, string shapeWkt, SpatialRelation relation, double distanceErrorPct = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct);

        /// <summary>
        ///     Matches documents with the value of specified field in relation with the provided WKT shape.
        /// </summary>
        /// <param name="fieldName">Spatial field name to get the value from.</param>
        /// <param name="shapeWkt">String representing the WKT shape.</param>
        /// <param name="relation">Spatial relation (Within, Contains, Disjoint, Intersects).</param>
        /// <param name="units">Units to be used (kilometers or miles).</param>
        /// <param name="distanceErrorPct">Allowed error percentage. Default: 0.025.</param>
        TSelf RelatesToShape(string fieldName, string shapeWkt, SpatialRelation relation, SpatialUnits units, double distanceErrorPct = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct);

        /// <summary>
        ///     Matches documents based on provided spatial criteria created by factory.
        /// </summary>
        /// <param name="path">Path to the spatial field to get value from.</param>
        /// <param name="clause">Function creating spatial criteria.</param>
        TSelf Spatial(Expression<Func<T, object>> path, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        /// <summary>
        ///     Matches documents based on provided spatial criteria created by factory.
        /// </summary>
        /// <param name="fieldName">Name of spatial field to get value from.</param>
        /// <param name="clause">Function creating spatial criteria.</param>
        TSelf Spatial(string fieldName, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        /// <summary>
        ///     Matches documents based on provided spatial criteria created by factory.
        /// </summary>
        /// <param name="field">Dynamic spatial field to get value from.</param>
        /// <param name="clause">Function creating spatial criteria.</param>
        TSelf Spatial(DynamicSpatialField field, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        /// <summary>
        ///     Matches documents based on provided factories.
        /// </summary>
        /// <param name="field">Function creating dynamic spatial field using values from chosen fields.</param>
        /// <param name="clause">Function creating spatial criteria.</param>
        TSelf Spatial(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        /// <inheritdoc cref="MoreLikeThisBase"/>
        /// <param name="moreLikeThis">Specified MoreLikeThisQuery.</param>
        TSelf MoreLikeThis(MoreLikeThisBase moreLikeThis);
        
        TSelf VectorSearch(Func<IVectorFieldFactory<T>, IVectorEmbeddingTextField> textFieldFactory, Action<IVectorEmbeddingTextFieldValueFactory> textValueFactory, float minimumSimilarity = Constants.VectorSearch.MinimumSimilarity);

        TSelf VectorSearch(Func<IVectorFieldFactory<T>, IVectorEmbeddingField> embeddingFieldFactory, Action<IVectorEmbeddingFieldValueFactory> embeddingValueFactory, float minimumSimilarity = Constants.VectorSearch.MinimumSimilarity);
        
        TSelf VectorSearch(Func<IVectorFieldFactory<T>, IVectorField> vectorFieldFactory, Action<IVectorFieldValueFactory> vectorValueFactory, float minimumSimilarity = Constants.VectorSearch.MinimumSimilarity);
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
