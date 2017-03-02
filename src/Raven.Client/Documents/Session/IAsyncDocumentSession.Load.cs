//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Transformers;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Interface for document session using async approaches
    /// </summary>
    public partial interface IAsyncDocumentSession
    {
        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        /// <param name="token">The cancellation token.</param>
        Task<T> LoadAsync<T>(string id, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        /// <param name="token">The cancellation token.</param>
        Task<Dictionary<string, T>> LoadAsync<T>(IEnumerable<string> ids, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified id
        /// </summary>
        /// <typeparam name="TTransformer">The transformer to use in this load operation</typeparam>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="id">Id of a document to load</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task<TResult> LoadAsync<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null, CancellationToken token = default(CancellationToken)) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified id
        /// </summary>
        /// <param name="ids">Enumerable of ids of documents to load</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task<Dictionary<string, TResult>> LoadAsync<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null, CancellationToken token = default(CancellationToken)) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified id
        /// </summary>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="id">Id of a document to load</param>
        /// <param name="transformer">The transformer to use in this load operation</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task<TResult> LoadAsync<TResult>(string id, string transformer, Action<ILoadConfiguration> configure = null, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified ids
        /// </summary>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="ids">Enumerable of ids of documents to load</param>
        /// <param name="transformer">The transformer to use in this load operation</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task<Dictionary<string, TResult>> LoadAsync<TResult>(IEnumerable<string> ids, string transformer, Action<ILoadConfiguration> configure = null, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified id
        /// </summary>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="id">Id of a entity to load</param>
        /// <param name="transformerType">The transformer to use in this load operation</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task<TResult> LoadAsync<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified ids
        /// </summary>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="ids">Enumerable of ids of documents to load</param>
        /// <param name="transformerType">The transformer to use in this load operation</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task<Dictionary<string, TResult>> LoadAsync<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure = null, CancellationToken token = default(CancellationToken));
    }
}
