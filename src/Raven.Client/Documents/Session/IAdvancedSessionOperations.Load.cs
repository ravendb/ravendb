//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface IAdvancedSessionOperations
    {
        /// <summary>
        ///     Check if document exists
        /// </summary>
        /// <param name="id">Document id</param>
        bool Exists(string id);

        /// <summary>
        ///     Loads multiple entities that contain common prefix.
        /// </summary>
        /// <param name="idPrefix">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document IDs (after 'idPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped. By default: 0.</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved. By default: 25.</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document IDs (after 'idPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="startAfter">
        ///     skip document fetching until given ID is found and return documents after that ID (default:
        ///     null)
        /// </param>
        T[] LoadStartingWith<T>(string idPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, string startAfter = null);

        /// <summary>
        ///     Loads multiple entities that contain common prefix into a given stream.
        /// </summary>
        /// <param name="idPrefix">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="output">the stream that will contain the load results</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document IDs (after 'idPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped. By default: 0.</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved. By default: 25.</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document IDs (after 'idPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="startAfter">
        ///     skip document fetching until given ID is found and return documents after that ID (default:
        ///     null)
        /// </param>
        void LoadStartingWithIntoStream(string idPrefix, Stream output, string matches = null, int start = 0, int pageSize = 25, string exclude = null, string startAfter = null);

        /// <summary>
        ///     Loads the specified entities with the specified ids directly into a given stream.
        /// </summary>
        /// <param name="ids">Enumerable of the Ids of the documents that should be loaded</param>
        /// <param name="output">the stream that will contain the load results</param>
        void LoadIntoStream(IEnumerable<string> ids, Stream output);
    }
}
