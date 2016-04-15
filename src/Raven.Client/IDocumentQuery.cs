//-----------------------------------------------------------------------
// <copyright file="IDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Client.Indexes;
using Raven.Client.Spatial;
using Raven.Json.Linq;

namespace Raven.Client
{
    /// <summary>
    ///     A query against a Raven index
    /// </summary>
    public interface IDocumentQuery<T> : IEnumerable<T>, IDocumentQueryBase<T, IDocumentQuery<T>>
    {
        /// <summary>
        ///     Whatever we should apply distinct operation to the query on the server side
        /// </summary>
        bool IsDistinct { get; }

        /// <summary>
        ///     Gets the query result. Accessing this property for the first time will execute the query.
        /// </summary>
        QueryResult QueryResult { get; }

        /// <summary>
        ///     Register the query as a lazy-count query in the session and return a lazy
        ///     instance that will evaluate the query only when needed.
        /// </summary>
        Lazy<int> CountLazily();

        /// <summary>
        ///     Get the facets as per the specified doc with the given start and pageSize
        /// </summary>
        /// <param name="queryInputs"></param>
        [Obsolete("Use SetTransformerParameters instead.")]
        IDocumentQuery<T> SetQueryInputs(Dictionary<string, RavenJToken> queryInputs);

        /// <summary>
        ///     Get the facets as per the specified facets with the given start and pageSize
        /// </summary>
        /// <param name="transformerParameters"></param>
        IDocumentQuery<T> SetTransformerParameters(Dictionary<string, RavenJToken> transformerParameters);

        /// <summary>
        ///     Create the index query object for this query
        /// </summary>
        IndexQuery GetIndexQuery(bool isAsync);

        /// <summary>
        ///     Register the query as a lazy query in the session and return a lazy
        ///     instance that will evaluate the query only when needed
        /// </summary>
        Lazy<IEnumerable<T>> Lazily();

        /// <summary>
        ///     Register the query as a lazy query in the session and return a lazy
        ///     instance that will evaluate the query only when needed.
        ///     Also provide a function to execute when the value is evaluated
        /// </summary>
        Lazy<IEnumerable<T>> Lazily(Action<IEnumerable<T>> onEval);

        /// <summary>
        ///     Selects the specified fields directly from the index if the are stored. If the field is not stored in index, value
        ///     will come from document directly.
        /// </summary>
        /// <typeparam name="TProjection">Type of the projection.</typeparam>
        /// <param name="fields">Array of fields to load.</param>
        IDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields);

        /// <summary>
        ///     Selects the specified fields directly from the index if the are stored. If the field is not stored in index, value
        ///     will come from document directly.
        /// </summary>
        /// <typeparam name="TProjection">Type of the projection.</typeparam>
        /// <param name="fields">Array of fields to load.</param>
        /// <param name="projections">Array of field projections.</param>
        IDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields, string[] projections);

        /// <summary>
        ///     Selects the specified fields directly from the index if the are stored. If the field is not stored in index, value
        ///     will come from document directly.
        ///     <para>Array of fields will be taken from TProjection</para>
        /// </summary>
        /// <typeparam name="TProjection">Type of the projection from which fields will be taken.</typeparam>
        IDocumentQuery<TProjection> SelectFields<TProjection>();
        
        /// <summary>
        ///     Ability to use one factory to determine spatial shape that will be used in query.
        /// </summary>
        /// <param name="path">Spatial field name.</param>
        /// <param name="clause">function with spatial criteria factory</param>
        IDocumentQuery<T> Spatial(Expression<Func<T, object>> path, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        /// <summary>
        ///     Ability to use one factory to determine spatial shape that will be used in query.
        /// </summary>
        /// <param name="fieldName">Spatial field name.</param>
        /// <param name="clause">function with spatial criteria factory</param>
        IDocumentQuery<T> Spatial(string fieldName, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        /// <summary>
        ///     Sets a transformer to use after executing a query
        /// </summary>
        /// <param name="resultsTransformer"></param>
        IDocumentQuery<TTransformerResult> SetResultTransformer<TTransformer, TTransformerResult>()
            where TTransformer : AbstractTransformerCreationTask, new();

    }
}
