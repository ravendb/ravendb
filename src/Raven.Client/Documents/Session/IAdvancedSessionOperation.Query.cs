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
    public partial interface IAdvancedSessionOperation
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
        /// <param name="indexName">Name of the index.</param>
        /// <param name="isMapReduce">Control how we treat identifier properties in map/reduce indexes</param>
        IDocumentQuery<T> DocumentQuery<T>(string indexName, bool isMapReduce = false);

        /// <summary>
        ///     Dynamically query RavenDB using Lucene syntax
        /// </summary>
        IDocumentQuery<T> DocumentQuery<T>();
    }
}
