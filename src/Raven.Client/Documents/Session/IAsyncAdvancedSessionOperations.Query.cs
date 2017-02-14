//-----------------------------------------------------------------------
// <copyright file="IAsyncAdvancedSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Indexes;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced async session operations
    /// </summary>
    public partial interface IAsyncAdvancedSessionOperations
    {
        /// <summary>
        ///     Queries the index specified by <typeparamref name="TIndexCreator" /> using lucene syntax.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        IAsyncDocumentQuery<T> AsyncDocumentQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new();

        /// <summary>
        ///     Query the specified index using Lucene syntax
        /// </summary>
        /// <param name="index">Name of the index.</param>
        /// <param name="isMapReduce">Control how we treat identifier properties in map/reduce indexes</param>
        IAsyncDocumentQuery<T> AsyncDocumentQuery<T>(string index, bool isMapReduce = false);

        /// <summary>
        ///     Dynamically query RavenDB using Lucene syntax
        /// </summary>
        IAsyncDocumentQuery<T> AsyncDocumentQuery<T>();
    }
}
