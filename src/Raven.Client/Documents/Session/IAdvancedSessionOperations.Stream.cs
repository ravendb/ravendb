//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Commands;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface IAdvancedSessionOperations
    {
        /// <summary>
        ///     Stream the results on the query to the client, converting them to
        ///     CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="query">Query to stream results for</param>
        IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query);

        /// <summary>
        ///     Stream the results on the query to the client, converting them to
        ///     CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="query">Query to stream results for</param>
        /// <param name="streamQueryStats">Information about the performed query</param>
        IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query, out StreamQueryStatistics streamQueryStats);

        /// <summary>
        ///     Stream the results on the query to the client, converting them to
        ///     CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="query">Query to stream results for</param>
        IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query);

        /// <summary>
        ///     Stream the results on the query to the client, converting them to
        ///     CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="query">Query to stream results for</param>
        /// <param name="streamQueryStats">Information about the performed query</param>
        IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats);

        /// <summary>
        ///     Stream the results of documents search to the client, converting them to CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="startsWith">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document ID (after 'idPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved</param>
        /// <param name="startAfter">
        ///     skip document fetching until given ID is found and return documents after that ID (default:
        ///     null)
        /// </param>
        IEnumerator<StreamResult<T>> Stream<T>(string startsWith, string matches = null, int start = 0, int pageSize = int.MaxValue, string startAfter = null);

        /// <summary>
        ///     Returns the results of a query directly into stream 
        /// </summary>
        void StreamInto<T>(IDocumentQuery<T> query, Stream output);

    }
}
