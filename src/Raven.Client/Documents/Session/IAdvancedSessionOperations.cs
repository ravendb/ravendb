//-----------------------------------------------------------------------
// <copyright file="IAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Indexes;
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
        ///     Updates entity with latest changes from server
        /// </summary>
        /// <param name="entity">Instance of an entity that will be refreshed</param>
        void Refresh<T>(T entity);

        /// <summary>
        /// Query the specified index using provided raw query
        /// </summary>
        /// <typeparam name="T">The query result type</typeparam>
        /// <param name="indexName">The index to query or null for dynamic</param>
        /// <returns></returns>
        IRawDocumentQuery<T> RawQuery<T>(string query, string indexName = null);

        /// <summary>
        /// Query the specified index using provided raw query
        /// </summary>
        /// <typeparam name="T">The query result type</typeparam>
        /// <typeparam name="TIndexCreator">The index creator task</typeparam>
        /// <param name="indexName">The index to query or null for dynamic</param>
        /// <returns></returns>
        IRawDocumentQuery<T> RawQuery<T, TIndexCreator>(string query) where TIndexCreator : AbstractIndexCreationTask, new();
    }
}
