using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Spatial;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Documents
{
    /// <summary>
    ///     Asynchronous query against a raven index
    /// </summary>
    public interface IAsyncDocumentQuery<T> : IDocumentQueryBase<T, IAsyncDocumentQuery<T>>
    {
        /// <summary>
        /// Register the query as a lazy-count query and return a lazy instance that will evaluate the query when needed.
        /// </summary>
        Lazy<Task<int>> CountLazilyAsync(CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Get the facets as per the specified doc with the given start and pageSize
        /// </summary>
        Task<FacetedQueryResult> GetFacetsAsync(string facetSetupDoc, int facetStart, int? facetPageSize, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Get the facets as per the specified facets with the given start and pageSize
        /// </summary>
        Task<FacetedQueryResult> GetFacetsAsync(List<Facet> facets, int facetStart, int? facetPageSize, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Create the index query object for this query
        /// </summary>
        IndexQuery GetIndexQuery(bool isAsync);

        /// <summary>
        ///     Register the query as a lazy query and return a lazy
        ///     instance that will evaluate the query only when needed.
        /// Also provide a function to execute when the value is evaluated
        /// </summary>
        Lazy<Task<IEnumerable<T>>> LazilyAsync(Action<IEnumerable<T>> onEval);

        /// <summary>
        ///     Gets the query result. Executing this method for the first time will execute the query.
        /// </summary>
        Task<QueryResult> QueryResultAsync(CancellationToken token = default (CancellationToken));

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
        /// <param name="fields">Array of fields to load.</param>
        /// <param name="projections">Array of field projections.</param>
        IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields, string[] projections);

        /// <summary>
        ///     Selects the specified fields directly from the index if the are stored. If the field is not stored in index, value
        ///     will come from document directly.
        ///     <para>Array of fields will be taken from TProjection</para>
        /// </summary>
        /// <typeparam name="TProjection">Type of the projection from which fields will be taken.</typeparam>
        IAsyncDocumentQuery<TProjection> SelectFields<TProjection>();

        /// <summary>
        ///     Transformer parameters that will be passed to transformer if one is specified.
        /// </summary>
        [Obsolete("Use SetTransformerParameters instead.")]
        void SetQueryInputs(Dictionary<string, object> queryInputs);

        /// <summary>
        ///     Transformer parameters that will be passed to transformer if one is specified.
        /// </summary>
        void SetTransformerParameters(Dictionary<string, object> transformerParameters);

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

        /// <summary>
        ///     Executed the query and returns the results.
        /// </summary>
        Task<IList<T>> ToListAsync(CancellationToken token = default (CancellationToken));
    }
}
