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
using Raven.Client.Documents.Session.Querying.Sharding;

namespace Raven.Client.Documents.Session
{
    public interface IAsyncDocumentQueryBase<T>
    {
        /// <summary>
        ///     Registers the lazy-count query and returns its lazy instance that will be evaluated on request.
        /// </summary>
        /// <param name="token">Cancellation token for this operation.</param>
        Lazy<Task<int>> CountLazilyAsync(CancellationToken token = default);

        /// <summary>
        ///     Registers the lazy-count query and returns its lazy instance that will be evaluated on request.
        /// </summary>
        /// <param name="token">Cancellation token for this operation.</param>
        Lazy<Task<long>> LongCountLazilyAsync(CancellationToken token = default);

        /// <summary>
        ///     Executes the query and returns the results as a list.
        /// </summary>
        /// <param name="token">Cancellation token for this operation.</param>
        Task<List<T>> ToListAsync(CancellationToken token = default);

        /// <summary>
        ///     Executes the query and returns the results as an array.
        /// </summary>
        /// <param name="token">Cancellation token for this operation.</param>
        Task<T[]> ToArrayAsync(CancellationToken token = default);

        /// <summary>
        ///     Returns first result of the query.
        /// </summary>
        /// <param name="token">Cancellation token for this operation.</param>
        /// <exception cref="InvalidOperationException">Results are empty.</exception>
        Task<T> FirstAsync(CancellationToken token = default);

        /// <summary>
        ///     Returns first result of the query, or default value for queried type if there are none.
        /// </summary>
        /// <param name="token">Cancellation token for this operation.</param>
        Task<T> FirstOrDefaultAsync(CancellationToken token = default);

        /// <summary>
        ///     Returns single result of the query.
        /// </summary>
        /// <param name="token">Cancellation token for this operation.</param>
        /// <exception cref="InvalidOperationException">There is not exactly one result.</exception>
        Task<T> SingleAsync(CancellationToken token = default);

        /// <summary>
        ///     Returns single result of the query, or default value for given type if sequence is empty.
        ///     Throws if there is more than one result.
        /// </summary>
        /// <param name="token">Cancellation token for this operation.</param>
        /// <exception cref="InvalidOperationException">There is more than a single result.</exception>
        Task<T> SingleOrDefaultAsync(CancellationToken token = default);

        /// <summary>
        ///     Checks if query returns any results.
        /// </summary>
        /// <param name="token">Cancellation token for this operation.</param>
        Task<bool> AnyAsync(CancellationToken token = default);

        /// <summary>
        ///     Gets the total count of query results.
        /// </summary>
        /// <param name="token">Cancellation token for this operation.</param>
        Task<int> CountAsync(CancellationToken token = default);

        /// <summary>
        ///     Gets the total count of query results as int64.
        /// </summary>
        /// <param name="token">Cancellation token for this operation.</param>
        Task<long> LongCountAsync(CancellationToken token = default);

        /// <summary>
        ///     Registers the lazy query and returns its lazy instance that will be evaluated on request.
        /// </summary>
        /// <param name="onEval">Action wrapping a method that will be executed on query evaluation. Default: null.</param>
        Lazy<Task<IEnumerable<T>>> LazilyAsync(Action<IEnumerable<T>> onEval = null);
    }

    /// <inheritdoc cref="IRawDocumentQuery{T}" />
    public interface IAsyncRawDocumentQuery<T> :
        IPagingDocumentQueryBase<T, IAsyncRawDocumentQuery<T>>,
        IQueryBase<T, IAsyncRawDocumentQuery<T>>,
        IAsyncDocumentQueryBase<T>
    {
        /// <inheritdoc cref="IRawDocumentQuery{T}.Projection" />
        IAsyncRawDocumentQuery<T> Projection(ProjectionBehavior projectionBehavior);
        
        /// <inheritdoc cref="IRawDocumentQuery{T}.ExecuteAggregation" />
        Task<Dictionary<string, FacetResult>> ExecuteAggregationAsync(CancellationToken token = default);
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

        ///<inheritdoc cref="IDocumentQuery{T}.GroupBy(string,string[])"/>
        IAsyncGroupByDocumentQuery<T> GroupBy(string fieldName, params string[] fieldNames);

        /// <inheritdoc cref="IAbstractDocumentQuery{T}.GroupBy(ValueTuple{string, GroupByMethod}, ValueTuple{string, GroupByMethod}[])" />
        IAsyncGroupByDocumentQuery<T> GroupBy((string Name, GroupByMethod Method) field, params (string Name, GroupByMethod Method)[] fields);

        /// <inheritdoc cref="IDocumentQuery{T}.MoreLikeThis(System.Action{Raven.Client.Documents.Queries.MoreLikeThis.IMoreLikeThisBuilderForDocumentQuery{T}})"/>
        IAsyncDocumentQuery<T> MoreLikeThis(Action<IMoreLikeThisBuilderForAsyncDocumentQuery<T>> builder);

        /// <inheritdoc cref="IDocumentQuery{T}.Filter"/>
        IAsyncDocumentQuery<T> Filter(Action<IFilterFactory<T>> builder, int limit = int.MaxValue);
        
        /// <inheritdoc cref="IAsyncAggregationDocumentQuery{T}.AndAggregateBy(System.Action{Raven.Client.Documents.Queries.Facets.IFacetBuilder{T}})"/>
        IAsyncAggregationDocumentQuery<T> AggregateBy(Action<IFacetBuilder<T>> builder);

        /// <inheritdoc cref="IAsyncAggregationDocumentQuery{T}.AndAggregateBy(FacetBase)" />
        IAsyncAggregationDocumentQuery<T> AggregateBy(FacetBase facet);

        /// <inheritdoc cref="IAsyncAggregationDocumentQuery{T}.AndAggregateBy(FacetBase)" />
        /// <param name="facets">List of FacetBase used to create aggregations.</param>
        IAsyncAggregationDocumentQuery<T> AggregateBy(IEnumerable<FacetBase> facets);

        /// <inheritdoc cref="IAsyncAggregationDocumentQuery{T}.AndAggregateBy(FacetBase)" />
        /// <param name="facetSetupDocumentId">ID of document where facet setup is stored.</param>
        IAsyncAggregationDocumentQuery<T> AggregateUsing(string facetSetupDocumentId);

        /// <inheritdoc cref="ISuggestionQuery{T}.AndSuggestUsing(SuggestionBase)"/>
        IAsyncSuggestionDocumentQuery<T> SuggestUsing(SuggestionBase suggestion);

        /// <inheritdoc cref="ISuggestionQuery{T}.AndSuggestUsing(Action{ISuggestionBuilder{T}})"/>
        IAsyncSuggestionDocumentQuery<T> SuggestUsing(Action<ISuggestionBuilder<T>> builder);

        IRavenQueryable<T> ToQueryable();

        /// <summary>
        /// It adds additional shard context to a query so it will be executed only on the relevant shards
        /// </summary>
        IAsyncDocumentQuery<T> ShardContext(Action<IQueryShardedContextBuilder> builder);
    }
}
