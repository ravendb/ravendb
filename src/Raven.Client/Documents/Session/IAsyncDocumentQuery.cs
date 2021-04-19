using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Client.Documents.Queries.TimeSeries;

namespace Raven.Client.Documents.Session
{
    public interface IAsyncDocumentQueryBase<T>
    {
        /// <summary>
        /// Register the query as a lazy-count query and return a lazy instance that will evaluate the query when needed.
        /// </summary>
        Lazy<Task<int>> CountLazilyAsync(CancellationToken token = default);

        /// <summary>
        ///     Executed the query and returns the results.
        /// </summary>
        Task<List<T>> ToListAsync(CancellationToken token = default);

        /// <summary>
        ///     Executed the query and returns the results.
        /// </summary>
        Task<T[]> ToArrayAsync(CancellationToken token = default);

        /// <summary>
        ///     Returns first element or throws if sequence is empty.
        /// </summary>
        Task<T> FirstAsync(CancellationToken token = default);

        /// <summary>
        ///     Returns first element or default value for type if sequence is empty.
        /// </summary>
        Task<T> FirstOrDefaultAsync(CancellationToken token = default);

        /// <summary>
        ///     Returns first element or throws if sequence is empty or contains more than one element.
        /// </summary>
        Task<T> SingleAsync(CancellationToken token = default);

        /// <summary>
        ///     Returns first element or default value for given type if sequence is empty. Throws if sequence contains more than
        ///     one element.
        /// </summary>
        Task<T> SingleOrDefaultAsync(CancellationToken token = default);

        /// <summary>
        ///     Checks if the given query matches any records
        /// </summary>
        Task<bool> AnyAsync(CancellationToken token = default);

        /// <summary>
        /// Gets the total count of records for this query
        /// </summary>
        Task<int> CountAsync(CancellationToken token = default);

        /// <summary>
        /// Gets the total count of records for this query in Long type
        /// </summary>
        Task<long> CountLongAsync(CancellationToken token = default);

        /// <summary>
        ///     Register the query as a lazy query and return a lazy
        ///     instance that will evaluate the query only when needed.
        /// Also provide a function to execute when the value is evaluated
        /// </summary>
        Lazy<Task<IEnumerable<T>>> LazilyAsync(Action<IEnumerable<T>> onEval = null);
    }

    public interface IAsyncRawDocumentQuery<T> :
        IQueryBase<T, IAsyncRawDocumentQuery<T>>,
        IAsyncDocumentQueryBase<T>
    {
        IAsyncRawDocumentQuery<T> Projection(ProjectionBehavior projectionBehavior);
    }

    public interface IAsyncGraphQuery<T> :
        IQueryBase<T, IAsyncGraphQuery<T>>,
        IAsyncDocumentQueryBase<T>
    {
        IAsyncGraphQuery<T> With<TOther>(string alias, IRavenQueryable<TOther> query);

        IAsyncGraphQuery<T> With<TOther>(string alias, string rawQuery);

        IAsyncGraphQuery<T> With<TOther>(string alias, Func<IAsyncDocumentQueryBuilder, IAsyncDocumentQuery<TOther>> queryFactory);

        IAsyncGraphQuery<T> WithEdges(string alias, string edgeSelector, string query);
    }

    /// <summary>
    ///     Asynchronous query against a raven index
    /// </summary>
    public interface IAsyncDocumentQuery<T> :
        IDocumentQueryBase<T, IAsyncDocumentQuery<T>>,
        IAsyncDocumentQueryBase<T>
    {
        string IndexName { get; }

        /// <summary>
        ///     Whether we should apply distinct operation to the query on the server side
        /// </summary>
        bool IsDistinct { get; }

        /// <summary>
        ///     Gets the query result. Executing this method for the first time will execute the query.
        /// </summary>
        Task<QueryResult> GetQueryResultAsync(CancellationToken token = default);

        /// <summary>
        ///     Selects the specified fields directly from the index if the are stored. If the field is not stored in index, value
        ///     will come from document directly.
        /// </summary>
        /// <typeparam name="TProjection">Type of the projection.</typeparam>
        /// <param name="fields">Array of fields to load.</param>
        IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields);

        /// <summary>
        ///     Selects the specified fields according to the given projection behavior.
        /// </summary>
        /// <typeparam name="TProjection">Type of the projection.</typeparam>
        /// <param name="projectionBehavior">Projection behavior to use.</param>
        /// <param name="fields">Array of fields to load.</param>
        IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(ProjectionBehavior projectionBehavior, params string[] fields);

        /// <summary>
        ///     Selects the specified fields.
        /// </summary>
        /// <typeparam name="TProjection">Type of the projection.</typeparam>
        /// <param name="queryData">An object containing the fields to load, field projections and a From-Token alias name</param>
        IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(QueryData queryData);

        /// <summary>
        ///     Selects the specified fields directly from the index if the are stored. If the field is not stored in index, value
        ///     will come from document directly.
        ///     <para>Array of fields will be taken from TProjection</para>
        /// </summary>
        /// <typeparam name="TProjection">Type of the projection from which fields will be taken.</typeparam>
        IAsyncDocumentQuery<TProjection> SelectFields<TProjection>();

        /// <summary>
        ///     Selects the specified fields according to the given projection behavior.
        ///     <para>Array of fields will be taken from TProjection</para>
        /// </summary>
        /// <typeparam name="TProjection">Type of the projection from which fields will be taken.</typeparam>
        /// <param name="projectionBehavior">Projection behavior to use.</param>
        IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(ProjectionBehavior projectionBehavior);

        /// <summary>
        ///     Selects a Time Series Aggregation based on
        ///     a time series query generated by an ITimeSeriesQueryBuilder.
        /// </summary>
        IAsyncDocumentQuery<TTimeSeries> SelectTimeSeries<TTimeSeries>(Func<ITimeSeriesQueryBuilder, TTimeSeries> timeSeriesQuery);

        /// <summary>
        /// Changes the return type of the query
        /// </summary>
        IAsyncDocumentQuery<TResult> OfType<TResult>();

        IAsyncGroupByDocumentQuery<T> GroupBy(string fieldName, params string[] fieldNames);

        IAsyncGroupByDocumentQuery<T> GroupBy((string Name, GroupByMethod Method) field, params (string Name, GroupByMethod Method)[] fields);

        IAsyncDocumentQuery<T> MoreLikeThis(Action<IMoreLikeThisBuilderForAsyncDocumentQuery<T>> builder);

        IAsyncAggregationDocumentQuery<T> AggregateBy(Action<IFacetBuilder<T>> builder);

        IAsyncAggregationDocumentQuery<T> AggregateBy(FacetBase facet);

        IAsyncAggregationDocumentQuery<T> AggregateBy(IEnumerable<Facet> facets);

        IAsyncAggregationDocumentQuery<T> AggregateUsing(string facetSetupDocumentId);

        IAsyncSuggestionDocumentQuery<T> SuggestUsing(SuggestionBase suggestion);

        IAsyncSuggestionDocumentQuery<T> SuggestUsing(Action<ISuggestionBuilder<T>> builder);

        IRavenQueryable<T> ToQueryable();
    }
}
