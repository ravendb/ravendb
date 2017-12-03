//-----------------------------------------------------------------------
// <copyright file="IRevisionsSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Client.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Revisions advanced synchronous session operations
    /// </summary>
    public interface IRevisionsSessionOperations
    {
        /// <summary>
        /// Returns all previous document revisions for specified document (with paging) ordered by most recent reivions first.
        /// </summary>
        List<T> GetFor<T>(string id, int start = 0, int pageSize = 25);

        /// <summary>
        /// Returns all previous document revisions metadata for specified document (with paging).
        /// </summary>
        List<MetadataAsDictionary> GetMetadataFor(string id, int start = 0, int pageSize = 25);

        /// <summary>
        /// Returns a document revision by change vector.
        /// </summary>
        T Get<T>(string changeVector);

        /// <summary>
        /// Returns document revisions by change vectors.
        /// </summary>
        Dictionary<string, T> Get<T>(IEnumerable<string> changeVectors);
    }
}
