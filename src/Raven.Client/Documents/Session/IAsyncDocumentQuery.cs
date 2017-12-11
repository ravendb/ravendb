using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Queries.Spatial;
using Raven.Client.Documents.Queries.Suggestions;

namespace Raven.Client.Documents.Session
{
    public interface IAsyncDocumentQueryBase<T> 
    {
        /// <summary>
        /// Register the query as a lazy-count query and return a lazy instance that will evaluate the query when needed.
        /// </summary>
        Lazy<Task<int>> CountLazilyAsync(CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Executed the query and returns the results.
        /// </summary>
        Task<List<T>> ToListAsync(CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Returns first element or throws if sequence is empty.
        /// </summary>
        Task<T> FirstAsync(CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Returns first element or default value for type if sequence is empty.
        /// </summary>
        Task<T> FirstOrDefaultAsync(CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Returns first element or throws if sequence is empty or contains more than one element.
        /// </summary>
        Task<T> SingleAsync(CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Returns first element or default value for given type if sequence is empty. Throws if sequence contains more than
        ///     one element.
        /// </summary>
        Task<T> SingleOrDefaultAsync(CancellationToken token = default(CancellationToken));

        /// <summary>
        /// Gets the total count of records for this query
        /// </summary>
        Task<int> CountAsync(CancellationToken token = default(CancellationToken));

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
        /// <summary>
        /// Add a named parameter to the query
        /// </summary>
        IAsyncDocumentQuery<T> AddParameter(string name, object value);
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
        ///     Gets the query result. Executing this method for the first time will execute the query.
        /// </summary>
        Task<QueryResult> GetQueryResultAsync(CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Selects the specified fields directly from the index if the are stored. If the field is not stored in index, value
        ///     will come from document directly.
        /// </summary>
        /// <typeparam name="TProjection">Type of the projection.</typeparam>
        /// <param name="fields">Array of fields to load.</param>
        IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields);

        /// <summary>
        ///     Selects the specified fields directly from the index if the are stored. If the field is not stored in index, value
        ///     will come from document directly.
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
        ///     Ability to use one factory to determine spatial shape that will be used in query.
        /// </summary>
        /// <param name="path">Spatial field name.</param>
        /// <param name="clause">function with spatial criteria factory</param>
        IAsyncDocumentQuery<T> Spatial(Expression<Func<T, object>> path, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        /// <summary>
        ///     Ability to use one factory to determine spatial shape that will be used in query.
        /// </summary>
        /// <param name="fieldName">Spatial field name.</param>
        /// <param name="clause">function with spatial criteria factory</param>
        IAsyncDocumentQuery<T> Spatial(string fieldName, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        IAsyncDocumentQuery<T> Spatial(DynamicSpatialField field, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        IAsyncDocumentQuery<T> Spatial(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        /// <summary>
        /// Changes the return type of the query
        /// </summary>
        IAsyncDocumentQuery<TResult> OfType<TResult>();

        IAsyncGroupByDocumentQuery<T> GroupBy(string fieldName, params string[] fieldNames);

        IAsyncDocumentQuery<T> MoreLikeThis(MoreLikeThisBase moreLikeThis);

        IAsyncDocumentQuery<T> MoreLikeThis(Action<IMoreLikeThisBuilderForAsyncDocumentQuery<T>> builder);

        IAsyncAggregationDocumentQuery<T> AggregateBy(Action<IFacetBuilder<T>> builder);

        IAsyncAggregationDocumentQuery<T> AggregateBy(FacetBase facet);

        IAsyncAggregationDocumentQuery<T> AggregateBy(IEnumerable<Facet> facets);

        IAsyncAggregationDocumentQuery<T> AggregateUsing(string facetSetupDocumentId);

        IAsyncSuggestionDocumentQuery<T> SuggestUsing(SuggestionBase suggestion);

        IAsyncSuggestionDocumentQuery<T> SuggestUsing(Action<ISuggestionBuilder<T>> builder);
    }
}
