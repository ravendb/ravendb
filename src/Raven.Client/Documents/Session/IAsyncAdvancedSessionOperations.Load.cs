//-----------------------------------------------------------------------
// <copyright file="IAsyncAdvancedSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Transformers;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced async session operations
    /// </summary>
    public partial interface IAsyncAdvancedSessionOperations
    {
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
        /// <param name="token">The cancellation token.</param>
        Task<IEnumerable<T>> LoadStartingWithAsync<T>(string idPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, string startAfter = null, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Loads multiple entities that contain common prefix and applies specified transformer.
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
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="startAfter">
        ///     skip document fetching until given ID is found and return documents after that ID (default:
        ///     null)
        /// </param>
        /// <param name="token">The cancellation token.</param>
        Task<Dictionary<string, TResult>> LoadStartingWithAsync<TTransformer, TResult>(string idPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, Action<ILoadConfiguration> configure = null, string startAfter = null, CancellationToken token = default(CancellationToken)) where TTransformer : AbstractTransformerCreationTask, new();

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
        /// <param name="token">The cancellation token.</param>
        Task LoadStartingWithIntoStreamAsync(string idPrefix, Stream output, string matches = null, int start = 0, int pageSize = 25, string exclude = null, string startAfter = null, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Loads multiple entities that contain common prefix and applies specified transformer.
        ///     and returns the results directly into a given stream
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
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="startAfter">
        ///     skip document fetching until given ID is found and return documents after that ID (default:
        ///     null)
        /// </param>
        /// <param name="token">The cancellation token.</param>
        Task LoadStartingWithIntoStreamAsync<TTransformer>(string idPrefix, Stream output, string matches = null, int start = 0, int pageSize = 25, string exclude = null, Action<ILoadConfiguration> configure = null, string startAfter = null, CancellationToken token = default(CancellationToken)) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        ///     Loads the specified entities with the specified ids directly into a given stream.
        /// </summary>
        /// <param name="ids">Enumerable of the Ids of the documents that should be loaded</param>
        /// <param name="output">the stream that will contain the load results</param>
        /// <param name="token">The cancellation token.</param>
        Task LoadIntoStreamAsync(IEnumerable<string> ids, Stream output, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified ids 
        ///     and returns the results directly into a given stream
        /// </summary>       
        /// <param name="ids">Enumerable of ids of documents to load</param>
        /// <param name="output">the stream that will contain the load results</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task LoadIntoStreamAsync<TTransformer>(IEnumerable<string> ids, Stream output,
            Action<ILoadConfiguration> configure = null, CancellationToken token = new CancellationToken())
            where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified ids
        ///     and returns the results directly into a given stream
        /// </summary>
        /// <param name="ids">Enumerable of ids of documents to load</param>
        /// <param name="transformer">The name of the transformer to use in this load operation</param>
        /// <param name="output">the stream that will contain the load results</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task LoadIntoStreamAsync(IEnumerable<string> ids, string transformer, Stream output,
            Action<ILoadConfiguration> configure = null, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified ids
        ///     and returns the results directly into a given stream
        /// </summary>
        /// <param name="ids">Enumerable of ids of documents to load</param>
        /// <param name="transformerType">The transformer to use in this load operation</param>
        /// <param name="output">the stream that will contain the load results</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task LoadIntoStreamAsync(IEnumerable<string> ids, Type transformerType, Stream output, Action<ILoadConfiguration> configure = null, CancellationToken token = default(CancellationToken));

    }
}
