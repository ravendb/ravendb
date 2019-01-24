//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Indexes;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface IAdvancedSessionOperations : IDocumentQueryBuilder
    {
    }

    public interface IDocumentQueryBuilder
    {
        /// <summary>
        ///     Queries the index specified by <typeparamref name="TIndexCreator" /> using lucene syntax.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        IDocumentQuery<T> DocumentQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new();

        /// <summary>
        ///     Query the specified index using Lucene syntax
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <param name="indexName">Name of the index (mutually exclusive with collectionName)</param>
        /// <param name="collectionName">Name of the collection (mutually exclusive with indexName)</param>
        /// <param name="isMapReduce">Whether we are querying a map/reduce index (modify how we treat identifier properties)</param>
        IDocumentQuery<T> DocumentQuery<T>(string indexName = null, string collectionName = null, bool isMapReduce = false);
    }
}
