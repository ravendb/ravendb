//-----------------------------------------------------------------------
// <copyright file="IAsyncAdvancedSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session.Operations.Lazy;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced async session operations
    /// </summary>
    public partial interface IAsyncAdvancedSessionOperations : IAdvancedDocumentSessionOperations
    {
        /// <summary>
        ///     Access the eager operations
        /// </summary>
        IAsyncEagerSessionOperations Eagerly { get; }

        /// <summary>
        ///     Access the lazy operations
        /// </summary>
        IAsyncLazySessionOperations Lazily { get; }

        /// <summary>
        ///     Returns full document url for a given entity
        /// </summary>
        /// <param name="entity">Instance of an entity for which url will be returned</param>
        string GetDocumentUrl(object entity);

        /// <summary>
        ///     Updates entity with latest changes from server
        /// </summary>
        /// <param name="entity">Instance of an entity that will be refreshed</param>
        /// <param name="token">The cancellation token.</param>
        Task RefreshAsync<T>(T entity, CancellationToken token = default (CancellationToken));
    }
}
