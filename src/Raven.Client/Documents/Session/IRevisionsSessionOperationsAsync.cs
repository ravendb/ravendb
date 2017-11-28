//-----------------------------------------------------------------------
// <copyright file="IRevisionsSessionOperationsAsync.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Revisions advanced async session operations
    /// </summary>
    public interface IRevisionsSessionOperationsAsync
    {
        /// <summary>
        /// Returns all previous document revisions for specified document (with paging).
        /// </summary>
        Task<List<T>> GetForAsync<T>(string id, int start = 0, int pageSize = 25);

        /// <summary>
        /// Returns all previous document revisions metadata for specified document (with paging).
        /// </summary>
        Task<List<MetadataAsDictionary>> GetMetadataForAsync(string id, int start = 0, int pageSize = 25);

        /// <summary>
        /// Returns a document revision by change vector.
        /// </summary>
        Task<T> GetAsync<T>(string changeVector);

        /// <summary>
        /// Returns document revisions by change vectors.
        /// </summary>
        Task<Dictionary<string, T>> GetAsync<T>(IEnumerable<string> changeVectors);
    }
}
