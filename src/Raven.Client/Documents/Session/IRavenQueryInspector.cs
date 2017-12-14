//-----------------------------------------------------------------------
// <copyright file="IRavenQueryInspector.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Provide access to the underlying <see cref="IDocumentQuery{T}"/>
    /// </summary>
    public interface IRavenQueryInspector
    {
        string IndexName { get; }

        /// <summary>
        /// The query session
        /// </summary>
        InMemoryDocumentSessionOperations Session { get; }

        /// <summary>
        /// Get the index query for this query
        /// </summary>
        IndexQuery GetIndexQuery(bool isAsync);
    }
}
