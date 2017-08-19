//-----------------------------------------------------------------------
// <copyright file="IAsyncAdvancedSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Util;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced async session operations
    /// </summary>
    public partial interface IAsyncAdvancedSessionOperations
    {
        /// <summary>
        ///     Stream the results on the query to the client, converting them to
        ///     CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="query">Query to stream results for</param>
        /// <param name="token">The cancellation token.</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Stream the results on the query to the client, converting them to
        ///     CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="query">Query to stream results for</param>
        /// <param name="token">The cancellation token.</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Stream the results of documents search to the client, converting them to CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="startsWith">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document IDs (after 'idPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved</param>
        /// <param name="startAfter">
        ///     skip document fetching until given ID is found and return documents after that ID (default:
        ///     null)
        /// </param>
        /// <param name="token">The cancellation token.</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(string startsWith, string matches = null, int start = 0, int pageSize = int.MaxValue, string startAfter = null, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Returns the results of a query directly into stream 
        /// </summary>
        Task StreamIntoAsync<T>(IAsyncDocumentQuery<T> query, Stream output, CancellationToken token = default(CancellationToken));

    }
}
