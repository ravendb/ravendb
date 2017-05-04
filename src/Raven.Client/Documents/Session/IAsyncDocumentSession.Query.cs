//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Interface for document session using async approaches
    /// </summary>
    public partial interface IAsyncDocumentSession
    {
        /// <summary>
        ///     Queries the specified index using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <param name="indexName">Name of the index.</param>
        /// <param name="isMapReduce">Whether we are querying a map/reduce index (modify how we treat identifier properties)</param>
        IRavenQueryable<T> Query<T>(string indexName, bool isMapReduce = false);

        /// <summary>
        ///     Dynamically queries RavenDB using LINQ
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        IRavenQueryable<T> Query<T>();

        /// <summary>
        ///     Queries the index specified by <typeparamref name="TIndexCreator" /> using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new();
    }
}
