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

    /// <summary>
    /// It gives the ability to construct queries with the usage of <see cref="IDocumentQuery{T}"/> interface
    /// </summary>
    public interface IDocumentQueryBuilder
    {
        /// <summary>
        /// It provides low-level querying capabilities with the usage of <typeparamref name="TIndexCreator" /> index.
        /// It gives a lot of flexibility and control over the process of building a query.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index to query</typeparam>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.WhatIsDocumentQuery"/>
        IDocumentQuery<T> DocumentQuery<T, TIndexCreator>() where TIndexCreator : AbstractCommonApiForIndexes, new();

        /// <summary>
        /// It provides low-level querying capabilities with the usage of an index or directly querying documents from a specified collection (note: it might require an auto index to be created).
        /// It gives a lot of flexibility and control over the process of building a query.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <param name="indexName">Name of the index to query (mutually exclusive with <paramref name="collectionName"/>)</param>
        /// <param name="collectionName">Name of the collection (mutually exclusive with <paramref name="indexName"/>)</param>
        /// <param name="isMapReduce">Whether we are querying a map/reduce index (modify how we treat identifier properties)</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.WhatIsDocumentQuery"/>
        IDocumentQuery<T> DocumentQuery<T>(string indexName = null, string collectionName = null, bool isMapReduce = false);
    }
}
