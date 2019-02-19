//-----------------------------------------------------------------------
// <copyright file="IAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Session.Operations.Lazy;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface IAdvancedSessionOperations : IAdvancedDocumentSessionOperations
    {
        /// <summary>
        ///     Access the eager operations
        /// </summary>
        IEagerSessionOperations Eagerly { get; }

        /// <summary>
        ///     Access the lazy operations
        /// </summary>
        ILazySessionOperations Lazily { get; }
        
        /// <summary>
        ///     Access the attachments operations
        /// </summary>
        IAttachmentsSessionOperations Attachments { get; }

        /// <summary>
        ///     Access the revisions operations
        /// </summary>
        IRevisionsSessionOperations Revisions { get; }
        
        /// <summary>
        ///     Access cluster transaction operations
        /// </summary>
        IClusterTransactionOperations ClusterTransaction { get; }

        /// <summary>
        ///     Updates entity with latest changes from server
        /// </summary>
        /// <param name="entity">Instance of an entity that will be refreshed</param>
        void Refresh<T>(T entity);

        /// <summary>
        /// Query the specified index using provided raw query
        /// </summary>
        /// <typeparam name="T">The query result type</typeparam>
        IRawDocumentQuery<T> RawQuery<T>(string query);

        /// <summary>
        /// Issue a graph query based on the raw match query provided
        /// </summary>
        /// <typeparam name="T">The query result type</typeparam>
        IGraphQuery<T> GraphQuery<T>(string query);
    }
}
