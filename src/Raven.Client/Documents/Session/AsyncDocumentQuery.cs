using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.Spatial;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Operations.Lazy;
using Raven.Client.Documents.Session.Tokens;
using Raven.Client.Documents.Transformers;
using Raven.Client.Extensions;
using Raven.Client.Util;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// A query against a Raven index
    /// </summary>
    public class AsyncDocumentQuery<T> : AbstractDocumentQuery<T, AsyncDocumentQuery<T>>, IAsyncDocumentQuery<T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncDocumentQuery{T}"/> class.
        /// </summary>
        public AsyncDocumentQuery(InMemoryDocumentSessionOperations session, string indexName, FieldsToFetchToken fieldsToFetchToken, bool isMapReduce)
            : base(session, indexName, fieldsToFetchToken, isMapReduce)
        {
        }

        /// <summary>
        /// Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name="path">The path.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Include(string path)
        {
            Include(path);
            return this;
        }

        /// <summary>
        /// Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name="path">The path.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Include(Expression<Func<T, object>> path)
        {
            Include(path);
            return this;
        }

        /// <summary>
        /// Negate the next operation
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Not
        {
            get
            {
                NegateNext();
                return this;
            }
        }

        /// <summary>
        /// Takes the specified count.
        /// </summary>
        /// <param name="count">The count.</param>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Take(int count)
        {
            Take(count);
            return this;
        }

        /// <summary>
        /// Skips the specified count.
        /// </summary>
        /// <param name="count">The count.</param>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Skip(int count)
        {
            Skip(count);
            return this;
        }

        /// <summary>
        /// Filter the results from the index using the specified where clause.
        /// </summary>
        /// <param name="whereClause">The where clause.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Where(string fieldName, string whereClause)
        {
            Where(fieldName, whereClause);
            return this;
        }

        /// <summary>
        /// 	Matches exact value
        /// </summary>
        /// <remarks>
        /// 	Defaults to NotAnalyzed
        /// </remarks>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEquals(string fieldName, object value)
        {
            WhereEquals(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches exact value
        /// </summary>
        /// <remarks>
        ///   Defaults to NotAnalyzed
        /// </remarks>
        public IAsyncDocumentQuery<T> WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereEquals(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// 	Matches exact value
        /// </summary>
        /// <remarks>
        /// 	Defaults to allow wildcards only if analyzed
        /// </remarks>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEquals(string fieldName, object value, bool isAnalyzed)
        {
            WhereEquals(fieldName, value, isAnalyzed);
            return this;
        }

        /// <summary>
        ///   Matches exact value
        /// </summary>
        /// <remarks>
        ///   Defaults to allow wildcards only if analyzed
        /// </remarks>
        public IAsyncDocumentQuery<T> WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value, bool isAnalyzed)
        {
            WhereEquals(GetMemberQueryPath(propertySelector.Body), value, isAnalyzed);
            return this;
        }

        /// <summary>
        /// Matches exact value
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEquals(WhereParams whereParams)
        {
            WhereEquals(whereParams);
            return this;
        }

        /// <summary>
        /// Check that the field has one of the specified value
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereIn(string fieldName, IEnumerable<object> values)
        {
            WhereIn(fieldName, values);
            return this;
        }

        /// <summary>
        /// Check that the field has one of the specified value
        /// </summary>
        public IAsyncDocumentQuery<T> WhereIn<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values)
        {
            WhereIn(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        /// <summary>
        /// Matches fields which starts with the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereStartsWith(string fieldName, object value)
        {
            WhereStartsWith(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields which starts with the specified value.
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        public IAsyncDocumentQuery<T> WhereStartsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereStartsWith(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Matches fields which ends with the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereEndsWith(string fieldName, object value)
        {
            WhereEndsWith(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields which ends with the specified value.
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        public IAsyncDocumentQuery<T> WhereEndsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereEndsWith(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is between the specified start and end, exclusive
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="start">The start.</param>
        /// <param name="end">The end.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereBetween(string fieldName, object start, object end)
        {
            WhereBetween(fieldName, start, end);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, exclusive
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        public IAsyncDocumentQuery<T> WhereBetween<TValue>(Expression<Func<T, TValue>> propertySelector, TValue start, TValue end)
        {
            WhereBetween(GetMemberQueryPath(propertySelector.Body), start, end);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereGreaterThan(string fieldName, object value)
        {
            WhereGreaterThan(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        public IAsyncDocumentQuery<T> WhereGreaterThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereGreaterThan(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is greater than or equal to the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereGreaterThanOrEqual(string fieldName, object value)
        {
            WhereGreaterThanOrEqual(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is greater than or equal to the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        public IAsyncDocumentQuery<T> WhereGreaterThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereGreaterThanOrEqual(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereLessThan(string fieldName, object value)
        {
            WhereLessThan(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        public IAsyncDocumentQuery<T> WhereLessThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereLessThan(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WhereLessThanOrEqual(string fieldName, object value)
        {
            WhereLessThanOrEqual(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        public IAsyncDocumentQuery<T> WhereLessThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereLessThanOrEqual(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Add an AND to the query
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.AndAlso()
        {
            AndAlso();
            return this;
        }

        /// <summary>
        /// Add an OR to the query
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrElse()
        {
            OrElse();
            return this;
        }

        /// <summary>
        /// Specifies a boost weight to the last where clause.
        /// The higher the boost factor, the more relevant the term will be.
        /// </summary>
        /// <param name="boost">boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight</param>
        /// <returns></returns>
        /// <remarks>
        /// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
        /// </remarks>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Boost(decimal boost)
        {
            Boost(boost);
            return this;
        }

        /// <summary>
        /// Specifies a fuzziness factor to the single word term in the last where clause
        /// </summary>
        /// <param name="fuzzy">0.0 to 1.0 where 1.0 means closer match</param>
        /// <returns></returns>
        /// <remarks>
        /// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
        /// </remarks>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Fuzzy(decimal fuzzy)
        {
            Fuzzy(fuzzy);
            return this;
        }

        /// <summary>
        /// Specifies a proximity distance for the phrase in the last where clause
        /// </summary>
        /// <param name="proximity">number of words within</param>
        /// <returns></returns>
        /// <remarks>
        /// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
        /// </remarks>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Proximity(int proximity)
        {
            Proximity(proximity);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.AlphaNumericOrdering(string fieldName, bool descending)
        {
            AlphaNumericOrdering(fieldName, descending);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.AlphaNumericOrdering<TResult>(Expression<Func<TResult, object>> propertySelector, bool descending)
        {
            var fieldName = GetMemberQueryPath(propertySelector);
            AlphaNumericOrdering(fieldName, descending);
            return this;
        }

        /// <summary>
        /// Order the search results randomly
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.RandomOrdering()
        {
            RandomOrdering();
            return this;
        }

        /// <summary>
        /// Order the search results randomly using the specified seed
        /// this is useful if you want to have repeatable random queries
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.RandomOrdering(string seed)
        {
            RandomOrdering(seed);
            return this;
        }

        /// <summary>
        /// Order the search results randomly
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.CustomSortUsing(string typeName, bool descending)
        {
            CustomSortUsing(typeName, descending);
            return this;
        }

        /// <summary>
        /// Filter matches to be inside the specified radius
        /// </summary>
        /// <param name="radius">The radius.</param>
        /// <param name="latitude">The latitude.</param>
        /// <param name="longitude">The longitude.</param>
        /// <param name="radiusUnits">The units of the <paramref name="radius"/>.</param>
        public IAsyncDocumentQuery<T> WithinRadiusOf(double radius, double latitude, double longitude, SpatialUnits radiusUnits = Indexes.Spatial.SpatialUnits.Kilometers)
        {
            return GenerateQueryWithinRadiusOf(Constants.Documents.Indexing.Fields.DefaultSpatialFieldName, radius, latitude, longitude, radiusUnits: radiusUnits);
        }

        /// <summary>
        /// Filter matches to be inside the specified radius
        /// </summary>
        public IAsyncDocumentQuery<T> WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, SpatialUnits radiusUnits = Indexes.Spatial.SpatialUnits.Kilometers)
        {
            return GenerateQueryWithinRadiusOf(fieldName, radius, latitude, longitude, radiusUnits: radiusUnits);
        }

        public IAsyncDocumentQuery<T> RelatesToShape(string fieldName, string shapeWKT, SpatialRelation rel, double distanceErrorPct = 0.025)
        {
            return GenerateSpatialQueryData(fieldName, shapeWKT, rel, distanceErrorPct);
        }

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SortByDistance()
        {
            OrderBy(Constants.Documents.Indexing.Fields.DistanceFieldName);
            return this;
        }

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SortByDistance(double lat, double lng)
        {
            OrderBy(string.Format("{0};{1};{2}", Constants.Documents.Indexing.Fields.DistanceFieldName, lat.ToInvariantString(), lng.ToInvariantString()));
            return this;
        }
        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SortByDistance(double lat, double lng, string spatialFieldName)
        {
            OrderBy(string.Format("{0};{1};{2};{3}", Constants.Documents.Indexing.Fields.DistanceFieldName, lat.ToInvariantString(), lng.ToInvariantString(), spatialFieldName));
            return this;
        }

        public IAsyncDocumentQuery<TResult> OfType<TResult>()
        {
            return CreateDocumentQueryInternal<TResult>(Transformer, FieldsToFetch, ProjectionFields);
        }

        IAsyncGroupByDocumentQuery<T> IAsyncDocumentQuery<T>.GroupBy(string fieldName, params string[] fieldNames)
        {
            GroupBy(fieldName, fieldNames);
            return new AsyncGroupByDocumentQuery<T>(this);
        }

        /// <summary>
        /// Order the results by the specified fields
        /// The fields are the names of the fields to sort, defaulting to sorting by ascending.
        /// You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name="fields">The fields.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderBy(string field, OrderingType ordering)
        {
            OrderBy(field, ordering);
            return this;
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name = "propertySelectors">Property selectors for the fields.</param>
        public IAsyncDocumentQuery<T> OrderBy<TValue>(params Expression<Func<T, TValue>>[] propertySelectors)
        {
            foreach (var item in propertySelectors)
            {
                OrderBy(GetMemberQueryPathForOrderBy(item), OrderingUtil.GetOrderingOfType(item.Type));
            }

            return this;
        }

        /// <summary>
        /// Order the results by the specified fields
        /// The fields are the names of the fields to sort, defaulting to sorting by descending.
        /// You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name="fields">The fields.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OrderByDescending(string field, OrderingType ordering)
        {
            OrderByDescending(field, ordering);
            return this;
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by descending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name = "propertySelectors">Property selectors for the fields.</param>
        public IAsyncDocumentQuery<T> OrderByDescending<TValue>(params Expression<Func<T, TValue>>[] propertySelectors)
        {
            foreach (var item in propertySelectors)
            {
                OrderByDescending(GetMemberQueryPathForOrderBy(item), OrderingUtil.GetOrderingOfType(item.Type));
            }

            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight(
            string fieldName, int fragmentLength, int fragmentCount, string fragmentsField)
        {
            Highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight(string fieldName, int fragmentLength, int fragmentCount,
            out FieldHighlightings fieldHighlightings)
        {
            Highlight(fieldName, fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight(string fieldName, string fieldKeyName, int fragmentLength, int fragmentCount,
            out FieldHighlightings fieldHighlightings)
        {
            Highlight(fieldName, fieldKeyName, fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight<TValue>(
            Expression<Func<T, TValue>> propertySelector,
            int fragmentLength,
            int fragmentCount,
            Expression<Func<T, IEnumerable>> fragmentsPropertySelector)
        {
            var fieldName = GetMemberQueryPath(propertySelector);
            var fragmentsField = GetMemberQueryPath(fragmentsPropertySelector);
            Highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight<TValue>(
            Expression<Func<T, TValue>> propertySelector, int fragmentLength, int fragmentCount,
            out FieldHighlightings fieldHighlightings)
        {
            Highlight(GetMemberQueryPath(propertySelector), fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Highlight<TValue>(
            Expression<Func<T, TValue>> propertySelector, Expression<Func<T, TValue>> keyPropertySelector, int fragmentLength, int fragmentCount,
            out FieldHighlightings fieldHighlightings)
        {
            Highlight(GetMemberQueryPath(propertySelector), GetMemberQueryPath(keyPropertySelector), fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SetHighlighterTags(string preTag, string postTag)
        {
            SetHighlighterTags(new[] { preTag }, new[] { postTag });
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SetHighlighterTags(string[] preTags, string[] postTags)
        {
            SetHighlighterTags(preTags, postTags);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of now.
        /// </summary>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResultsAsOfNow()
        {
            WaitForNonStaleResultsAsOfNow();
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of now for the specified timeout.
        /// </summary>
        /// <param name="waitTimeout">The wait timeout.</param>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOfNow(waitTimeout);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag.
        /// </summary>
        /// <param name="cutOffEtag">The cut off etag.</param>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResultsAsOf(long cutOffEtag)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag for the specified timeout.
        /// </summary>
        /// <param name="cutOffEtag">The cut off etag.</param>
        /// <param name="waitTimeout">The wait timeout.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResultsAsOf(long cutOffEtag, TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag, waitTimeout);
            return this;
        }

        /// <summary>
        /// EXPERT ONLY: Instructs the query to wait for non stale results.
        /// This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResults()
        {
            WaitForNonStaleResults();
            return this;
        }

        /// <summary>
        /// Allows you to modify the index query before it is sent to the server
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.BeforeQueryExecution(Action<IndexQuery> beforeQueryExecution)
        {
            BeforeQueryExecution(beforeQueryExecution);
            return this;
        }

        /// <summary>
        /// EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
        /// This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        /// <param name="waitTimeout">The wait timeout.</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.WaitForNonStaleResults(TimeSpan waitTimeout)
        {
            WaitForNonStaleResults(waitTimeout);
            return this;
        }

        /// <summary>
        /// Selects all the projection fields directly from the index
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        public IAsyncDocumentQuery<TProjection> SelectFields<TProjection>()
        {
            return SelectFields<TProjection>(ReflectionUtil.GetPropertiesAndFieldsFor<TProjection>(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public).Select(x => x.Name).ToArray());
        }

        /// <summary>
        /// Selects the specified fields directly from the index
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        public IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields)
        {
            return SelectFields<TProjection>(fields, fields);
        }

        /// <summary>
        /// Selects the specified fields directly from the index
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        public IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields, string[] projections)
        {
            return CreateDocumentQueryInternal<TProjection>(Transformer, fields, projections);
        }

        public IAsyncDocumentQuery<T> Spatial(Expression<Func<T, object>> path, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            return Spatial(path.ToPropertyPath(), clause);
        }

        public IAsyncDocumentQuery<T> Spatial(string fieldName, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            var criteria = clause(new SpatialCriteriaFactory());
            return GenerateSpatialQueryData(fieldName, criteria);
        }

        public IAsyncDocumentQuery<T> SetTransformerParameters(Dictionary<string, object> parameters)
        {
            TransformerParameters = parameters;
            return this;
        }

        /// <summary>
        /// Register the query as a lazy-count query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public Lazy<Task<int>> CountLazilyAsync(CancellationToken token = default(CancellationToken))
        {
            if (QueryOperation == null)
            {
                Take(0);
                QueryOperation = InitializeQueryOperation();
            }

            var lazyQueryOperation = new LazyQueryOperation<T>(TheSession.Conventions, QueryOperation, AfterQueryExecutedCallback);

            return ((AsyncDocumentSession)TheSession).AddLazyCountOperation(lazyQueryOperation, token);
        }

        /// <summary>
        /// Adds an ordering for a specific field to the query
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="descending">if set to <c>true</c> [descending].</param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.AddOrder(string fieldName, bool descending)
        {
            AddOrder(fieldName, descending);
            return this;
        }

        /// <summary>
        /// Adds an ordering by score for a specific field to the query
        /// </summary>
        public IAsyncDocumentQuery<T> OrderByScore()
        {
            AddOrder(Constants.Documents.Indexing.Fields.IndexFieldScoreName, false);
            return this;
        }

        /// <summary>
        /// Adds an ordering by score descending for a specific field to the query
        /// </summary>
        public IAsyncDocumentQuery<T> OrderByScoreDescending()
        {
            AddOrder(Constants.Documents.Indexing.Fields.IndexFieldScoreName, true);
            return this;
        }

        /// <summary>
        ///   Adds an ordering for a specific field to the query
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "descending">if set to <c>true</c> [descending].</param>
        public IAsyncDocumentQuery<T> AddOrder<TValue>(Expression<Func<T, TValue>> propertySelector, bool descending)
        {
            AddOrder(GetMemberQueryPath(propertySelector.Body), descending);
            return this;
        }

        /// <summary>
        /// Simplified method for opening a new clause within the query
        /// </summary>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.OpenSubclause()
        {
            OpenSubclause();
            return this;
        }

        /// <summary>
        /// Perform a search for documents which fields that match the searchTerms.
        /// If there is more than a single term, each of them will be checked independently.
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Search(string fieldName, string searchTerms, EscapeQueryOptions escapeQueryOptions)
        {
            Search(fieldName, searchTerms, escapeQueryOptions);
            return this;
        }

        /// <summary>
        /// Perform a search for documents which fields that match the searchTerms.
        /// If there is more than a single term, each of them will be checked independently.
        /// </summary>
        public IAsyncDocumentQuery<T> Search<TValue>(Expression<Func<T, TValue>> propertySelector, string searchTerms, EscapeQueryOptions escapeQueryOptions = EscapeQueryOptions.RawQuery)
        {
            Search(GetMemberQueryPath(propertySelector.Body), searchTerms, escapeQueryOptions);
            return this;
        }

        /// <summary>
        /// Simplified method for closing a clause within the query
        /// </summary>
        /// <returns></returns>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.CloseSubclause()
        {
            CloseSubclause();
            return this;
        }

        /// <summary>
        /// Partition the query so we can intersect different parts of the query
        /// across different index entries.
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Intersect()
        {
            Intersect();
            return this;
        }

        /// <summary>
        /// Performs a query matching ANY of the provided values against the given field (OR)
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.ContainsAny(string fieldName, IEnumerable<object> values)
        {
            ContainsAny(fieldName, values);
            return this;
        }

        /// <summary>
        /// Performs a query matching ANY of the provided values against the given field (OR)
        /// </summary>
        public IAsyncDocumentQuery<T> ContainsAny<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values)
        {
            ContainsAny(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        /// <summary>
        /// Performs a query matching ALL of the provided values against the given field (AND)
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.ContainsAll(string fieldName, IEnumerable<object> values)
        {
            ContainsAll(fieldName, values);
            return this;
        }

        /// <summary>
        /// Performs a query matching ALL of the provided values against the given field (AND)
        /// </summary>
        public IAsyncDocumentQuery<T> ContainsAll<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values)
        {
            ContainsAll(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        /// <summary>
        /// Provide statistics about the query, such as total count of matching records
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Statistics(out QueryStatistics stats)
        {
            Statistics(out stats);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.UsingDefaultOperator(QueryOperator queryOperator)
        {
            UsingDefaultOperator(queryOperator);
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.NoTracking()
        {
            NoTracking();
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.NoCaching()
        {
            NoCaching();
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.ShowTimings()
        {
            ShowTimings();
            return this;
        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.Distinct()
        {
            Distinct();
            return this;
        }

        /// <summary>
        /// Sets a transformer to use after executing a query
        /// </summary>
        /// <param name="transformer"></param>
        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SetTransformer(string transformer)
        {
            SetTransformer(transformer);
            return this;
        }

        public IAsyncDocumentQuery<T> ExplainScores()
        {
            ShouldExplainScores = true;
            return this;

        }

        IAsyncDocumentQuery<T> IDocumentQueryBase<T, IAsyncDocumentQuery<T>>.SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(bool val)
        {
            SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(val);
            return this;
        }

        public IAsyncDocumentQuery<TTransformerResult> SetTransformer<TTransformer, TTransformerResult>() where TTransformer : AbstractTransformerCreationTask, new()
        {
            return CreateDocumentQueryInternal<TTransformerResult>(new TTransformer().TransformerName, FieldsToFetch, ProjectionFields);
        }

        public async Task<FacetedQueryResult> GetFacetsAsync(string facetSetupDoc, int facetStart, int? facetPageSize, CancellationToken token = default(CancellationToken))
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(IndexName, q, facetSetupDoc, null, facetStart, facetPageSize, Conventions);

            var command = new GetFacetsCommand(TheSession.Context, query);
            await TheSession.RequestExecutor.ExecuteAsync(command, TheSession.Context, token).ConfigureAwait(false);

            return command.Result;
        }

        public async Task<FacetedQueryResult> GetFacetsAsync(List<Facet> facets, int facetStart, int? facetPageSize, CancellationToken token = default(CancellationToken))
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(IndexName, q, null, facets, facetStart, facetPageSize, Conventions);

            var command = new GetFacetsCommand(TheSession.Context, query);
            await TheSession.RequestExecutor.ExecuteAsync(command, TheSession.Context, token).ConfigureAwait(false);

            return command.Result;
        }

        public Lazy<Task<FacetedQueryResult>> GetFacetsLazyAsync(string facetSetupDoc, int facetStart, int? facetPageSize, CancellationToken token = default(CancellationToken))
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(IndexName, q, facetSetupDoc, null, facetStart, facetPageSize, Conventions);

            var lazyFacetsOperation = new LazyFacetsOperation(query);
            return ((AsyncDocumentSession)TheSession).AddLazyOperation<FacetedQueryResult>(lazyFacetsOperation, null, token);
        }

        public Lazy<Task<FacetedQueryResult>> GetFacetsLazyAsync(List<Facet> facets, int facetStart, int? facetPageSize, CancellationToken token = default(CancellationToken))
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(IndexName, q, null, facets, facetStart, facetPageSize, Conventions);

            var lazyFacetsOperation = new LazyFacetsOperation(query);
            return ((AsyncDocumentSession)TheSession).AddLazyOperation<FacetedQueryResult>(lazyFacetsOperation, null, token);
        }

        /// <summary>
        /// Returns a list of results for a query asynchronously. 
        /// </summary>
        public async Task<IList<T>> ToListAsync(CancellationToken token = default(CancellationToken))
        {
            await InitAsync(token).ConfigureAwait(false);
            var tuple = await ProcessEnumerator(QueryOperation).WithCancellation(token).ConfigureAwait(false);
            return tuple.Item2;
        }

        public async Task<T> FirstAsync(CancellationToken token = default(CancellationToken))
        {
            var operation = await ExecuteQueryOperation(1, token).ConfigureAwait(false);
            return operation.First();
        }

        public async Task<T> FirstOrDefaultAsync(CancellationToken token = default(CancellationToken))
        {
            var operation = await ExecuteQueryOperation(1, token).ConfigureAwait(false);
            return operation.FirstOrDefault();
        }

        public async Task<T> SingleAsync(CancellationToken token = default(CancellationToken))
        {
            var operation = await ExecuteQueryOperation(2, token).ConfigureAwait(false);
            return operation.Single();
        }

        public async Task<T> SingleOrDefaultAsync(CancellationToken token = default(CancellationToken))
        {
            var operation = await ExecuteQueryOperation(2, token).ConfigureAwait(false);
            return operation.SingleOrDefault();
        }

        private async Task<IEnumerable<T>> ExecuteQueryOperation(int take, CancellationToken token)
        {
            if (PageSize.HasValue == false || PageSize > take)
                Take(take);

            await InitAsync(token).ConfigureAwait(false);

            return QueryOperation.Complete<T>();
        }

        /// <summary>
        /// Register the query as a lazy query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public Lazy<Task<IEnumerable<T>>> LazilyAsync(Action<IEnumerable<T>> onEval = null)
        {
            if (QueryOperation == null)
            {
                QueryOperation = InitializeQueryOperation();
            }

            var lazyQueryOperation = new LazyQueryOperation<T>(TheSession.Conventions, QueryOperation, AfterQueryExecutedCallback);
            return ((AsyncDocumentSession)TheSession).AddLazyOperation(lazyQueryOperation, onEval);
        }

        /// <summary>
        /// Gets the total count of records for this query
        /// </summary>
        public async Task<int> CountAsync(CancellationToken token = default(CancellationToken))
        {
            Take(0);
            var result = await QueryResultAsync(token).ConfigureAwait(false);
            return result.TotalResults;
        }

        private static Task<Tuple<QueryResult, IList<T>>> ProcessEnumerator(QueryOperation currentQueryOperation)
        {
            var list = currentQueryOperation.Complete<T>();
            return Task.FromResult(Tuple.Create(currentQueryOperation.CurrentQueryResults, list));
        }

        /// <summary>
        ///   Gets the query result
        ///   Execute the query the first time that this is called.
        /// </summary>
        /// <value>The query result.</value>
        public async Task<QueryResult> QueryResultAsync(CancellationToken token = default(CancellationToken))
        {
            await InitAsync(token).ConfigureAwait(false);

            return QueryOperation.CurrentQueryResults.CreateSnapshot();
        }

        protected virtual async Task InitAsync(CancellationToken token)
        {
            if (QueryOperation != null)
                return;

            var beforeQueryExecutedEventArgs = new BeforeQueryExecutedEventArgs(TheSession, this);
            TheSession.OnBeforeQueryExecutedInvoke(beforeQueryExecutedEventArgs);

            QueryOperation = InitializeQueryOperation();
            await ExecuteActualQueryAsync(token).ConfigureAwait(false);
        }

        private async Task ExecuteActualQueryAsync(CancellationToken token)
        {
            using (QueryOperation.EnterQueryContext())
            {
                QueryOperation.LogQuery();
                var command = QueryOperation.CreateRequest();
                await TheSession.RequestExecutor.ExecuteAsync(command, TheSession.Context, token).ConfigureAwait(false);
                QueryOperation.SetResult(command.Result);
            }

            InvokeAfterQueryExecuted(QueryOperation.CurrentQueryResults);
        }

        private AsyncDocumentQuery<TResult> CreateDocumentQueryInternal<TResult>(string transformer, string[] fieldsToFetch, string[] projectionFields)
        {
            var query = new AsyncDocumentQuery<TResult>(
                TheSession,
                IndexName,
                null,
                IsMapReduce)
            {
                PageSize = PageSize,
                WhereTokens = new LinkedList<QueryToken>(WhereTokens.Select(x => x.Clone())),
                OrderByTokens = new LinkedList<QueryToken>(OrderByTokens.Select(x => x.Clone())),
                GroupByTokens = new LinkedList<QueryToken>(GroupByTokens.Select(x => x.Clone())),
                QueryParameters = QueryParameters,
                Start = Start,
                Timeout = Timeout,
                CutoffEtag = CutoffEtag,
                QueryStats = QueryStats,
                TheWaitForNonStaleResults = TheWaitForNonStaleResults,
                TheWaitForNonStaleResultsAsOfNow = TheWaitForNonStaleResultsAsOfNow,
                AllowMultipleIndexEntriesForSameDocumentToResultTransformer = AllowMultipleIndexEntriesForSameDocumentToResultTransformer,
                Negate = Negate,
                TransformResultsFunc = TransformResultsFunc,
                Includes = new HashSet<string>(Includes),
                IsSpatialQuery = IsSpatialQuery,
                SpatialFieldName = SpatialFieldName,
                QueryShape = QueryShape,
                SpatialRelation = SpatialRelation,
                SpatialUnits = SpatialUnits,
                DistanceErrorPct = DistanceErrorPct,
                RootTypes = { typeof(T) },
                BeforeQueryExecutionAction = BeforeQueryExecutionAction,
                AfterQueryExecutedCallback = AfterQueryExecutedCallback,
                AfterStreamExecutedCallback = AfterStreamExecutedCallback,
                HighlightedFields = new List<HighlightedField>(HighlightedFields),
                HighlighterPreTags = HighlighterPreTags,
                HighlighterPostTags = HighlighterPostTags,
                Transformer = transformer,
                TransformerParameters = TransformerParameters,
                DisableEntitiesTracking = DisableEntitiesTracking,
                DisableCaching = DisableCaching,
                ShowQueryTimings = ShowQueryTimings,
                LastEquality = LastEquality,
                ShouldExplainScores = ShouldExplainScores
            };

            FieldsToFetchToken fieldsToFetchToken = null;
            if (fieldsToFetch != null && fieldsToFetch.Length > 0)
                fieldsToFetchToken = FieldsToFetchToken.Create(fieldsToFetch, projectionFields);

            foreach (var token in SelectTokens)
            {
                if (fieldsToFetchToken == null)
                {
                    query.SelectTokens.AddLast(token.Clone());
                    continue;
                }

                if (token is FieldsToFetchToken)
                {
                    query.SelectTokens.AddLast(fieldsToFetchToken);
                    continue;
                }

                query.SelectTokens.AddLast(fieldsToFetchToken);
            }

            query.AfterQueryExecuted(AfterQueryExecutedCallback);
            return query;
        }
    }
}
