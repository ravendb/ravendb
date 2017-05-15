//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Transformers;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface IAdvancedSessionOperation
    {
        /// <summary>
        ///     Check if document exists
        /// </summary>
        /// <param name="id">Document id</param>
        bool Exists(string id);

        /// <summary>
        ///     Loads multiple entities that contain common prefix.
        /// </summary>
        /// <param name="keyPrefix">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped. By default: 0.</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved. By default: 25.</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="startAfter">
        ///     skip document fetching until given key is found and return documents after that key (default:
        ///     null)
        /// </param>
        T[] LoadStartingWith<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, string startAfter = null);

        /// <summary>
        ///     Loads multiple entities that contain common prefix and applies specified transformer.
        /// </summary>
        /// <param name="keyPrefix">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped. By default: 0.</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved. By default: 25.</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="startAfter">
        ///     skip document fetching until given key is found and return documents after that key (default:
        ///     null)
        /// </param>
        Dictionary<string, TResult> LoadStartingWith<TTransformer, TResult>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, Action<ILoadConfiguration> configure = null, string startAfter = null) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        ///     Loads multiple entities that contain common prefix into a given stream.
        /// </summary>
        /// <param name="keyPrefix">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="output">the strem that will contain the load results</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped. By default: 0.</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved. By default: 25.</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="startAfter">
        ///     skip document fetching until given key is found and return documents after that key (default:
        ///     null)
        /// </param>
        void LoadStartingWithIntoStream(string keyPrefix, Stream output, string matches = null, int start = 0, int pageSize = 25, string exclude = null, string startAfter = null);

        /// <summary>
        ///     Loads multiple entities that contain common prefix and applies specified transformer.
        ///     and returns the results directly into a given stream
        /// </summary>
        /// <param name="keyPrefix">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="output">the strem that will contain the load results</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped. By default: 0.</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved. By default: 25.</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="startAfter">
        ///     skip document fetching until given key is found and return documents after that key (default:
        ///     null)
        /// </param>
        void LoadStartingWithIntoStream<TTransformer>(string keyPrefix, Stream output, string matches = null, int start = 0, int pageSize = 25, string exclude = null, Action<ILoadConfiguration> configure = null, string startAfter = null) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        ///     Loads the specified entities with the specified ids directly into a given stream.
        /// </summary>
        /// <param name="ids">Enumerable of the Ids of the documents that should be loaded</param>
        /// <param name="output">the strem that will contain the load results</param>
        void LoadIntoStream(IEnumerable<string> ids, Stream output);

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified ids
        ///     and returns the results directly into a given stream
        /// </summary>
        /// <param name="ids">Enumerable of ids of documents to load</param>
        /// <param name="output">the strem that will contain the load results</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        void LoadIntoStream<TTransformer>(IEnumerable<string> ids, Stream output, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified ids
        ///     and returns the results directly into a given stream
        /// </summary>
        /// <param name="ids">Enumerable of ids of documents to load</param>
        /// <param name="transformer">The transformer to use in this load operation</param>
        /// <param name="output">the strem that will contain the load results</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        void LoadIntoStream(IEnumerable<string> ids, string transformer, Stream output, Action<ILoadConfiguration> configure = null);

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified ids
        ///     and returns the results directly into a given stream
        /// </summary>
        /// <param name="ids">Enumerable of ids of documents to load</param>
        /// <param name="transformerType">The transformer to use in this load operation</param>
        /// <param name="output">the strem that will contain the load results</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        void LoadIntoStream(IEnumerable<string> ids, Type transformerType, Stream output, Action<ILoadConfiguration> configure = null);
    }
}
