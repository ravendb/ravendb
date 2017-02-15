//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface IAdvancedSessionOperation
    {
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
        /// <param name="pagingInformation">used to perform rapid pagination on a server side</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="skipAfter">
        ///     skip document fetching until given key is found and return documents after that key (default:
        ///     null)
        /// </param>
        T[] LoadStartingWith<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, PagingInformation pagingInformation = null, string skipAfter = null);

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
        /// <param name="pagingInformation">used to perform rapid pagination on a server side</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="skipAfter">
        ///     skip document fetching until given key is found and return documents after that key (default:
        ///     null)
        /// </param>
        Dictionary<string, TResult> LoadStartingWith<TTransformer, TResult>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, PagingInformation pagingInformation = null, Action<ILoadConfiguration> configure = null, string skipAfter = null) where TTransformer : AbstractTransformerCreationTask, new();
    }
}
