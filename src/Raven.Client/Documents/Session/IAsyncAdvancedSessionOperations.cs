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
        ///     Access the attachments operations
        /// </summary>
        IAttachmentsSessionOperationsAsync Attachments { get; }

        /// <summary>
        ///     Access the revisions operations
        /// </summary>
        IRevisionsSessionOperationsAsync Revisions { get; }

        /// <summary>
        ///     Access cluster transaction operations
        /// </summary>
        IClusterTransactionOperationsAsync ClusterTransaction { get; }

        /// <summary>
        ///     Updates entity with latest changes from server
        /// </summary>
        /// <param name="entity">Instance of an entity that will be refreshed</param>
        /// <param name="token">The cancellation token.</param>
        Task RefreshAsync<T>(T entity, CancellationToken token = default (CancellationToken));

        /// <summary>
        /// Query the specified index using provided raw query
        /// </summary>
        /// <typeparam name="T">The query result type</typeparam>
        IAsyncRawDocumentQuery<T> AsyncRawQuery<T>(string query);

        /// <summary>
        /// Issue a graph query based on the raw match query provided
        /// </summary>
        /// <typeparam name="T">The query result type</typeparam>
        IAsyncGraphQuery<T> AsyncGraphQuery<T>(string query);
    }
}
