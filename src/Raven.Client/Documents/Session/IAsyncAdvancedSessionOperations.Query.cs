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
    public partial interface IAsyncAdvancedSessionOperations : IAsyncDocumentQueryBuilder
    {
    }

    /// <summary>
    /// It gives the ability to construct queries with the usage of <see cref="IAsyncDocumentQuery{T}"/> interface
    /// </summary>
    public interface IAsyncDocumentQueryBuilder
    {
        /// <inheritdoc cref="IDocumentQueryBuilder.DocumentQuery{T,TIndexCreator}"/>
        IAsyncDocumentQuery<T> AsyncDocumentQuery<T, TIndexCreator>() where TIndexCreator : AbstractCommonApiForIndexes, new();

        /// <inheritdoc cref="IDocumentQueryBuilder.DocumentQuery{T}"/>
        IAsyncDocumentQuery<T> AsyncDocumentQuery<T>(string indexName = null, string collectionName = null, bool isMapReduce = false);
    }
}
