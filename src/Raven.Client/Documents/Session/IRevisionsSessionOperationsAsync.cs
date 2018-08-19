//-----------------------------------------------------------------------
// <copyright file="IRevisionsSessionOperationsAsync.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
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
        /// Returns the first revision for this document that happens before or at
        /// the specified date
        /// </summary>
        Task<T> GetBeforeAsync<T>(string id, DateTime date, CancellationToken token = default);

        /// <summary>
        /// Returns all previous document revisions for specified document (with paging).
        /// </summary>
        Task<List<T>> GetForAsync<T>(string id, int start = 0, int pageSize = 25, CancellationToken token = default);

        /// <summary>
        /// Returns all previous document revisions metadata for specified document (with paging).
        /// </summary>
        Task<List<MetadataAsDictionary>> GetMetadataForAsync(string id, int start = 0, int pageSize = 25, CancellationToken token = default);

        /// <summary>
        /// Returns a document revision by change vector.
        /// </summary>
        Task<T> GetAsync<T>(string changeVector, CancellationToken token = default);

        /// <summary>
        /// Returns document revisions by change vectors.
        /// </summary>
        Task<Dictionary<string, T>> GetAsync<T>(IEnumerable<string> changeVectors, CancellationToken token = default);
    }
}
