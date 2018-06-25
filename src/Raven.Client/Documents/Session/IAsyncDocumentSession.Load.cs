//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session.Loaders;

namespace Raven.Client.Documents.Session
{
    /// <inheritdoc />
    public partial interface IAsyncDocumentSession
    {
        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        /// <param name="token">The cancellation token.</param>
        Task<T> LoadAsync<T>(string id, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        /// <param name="token">The cancellation token.</param>
        Task<Dictionary<string, T>> LoadAsync<T>(IEnumerable<string> ids, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Loads the specified entity with the specified id, 
        ///     and includes other Documents and/or Counters.
        /// </summary>
        /// <param name="includes">
        ///     An action that specifies which documents and\or counters 
        ///     to include, by using the IIncludeBuilder interface.
        /// </param>
        /// <param name="token">The cancellation token.</param>

        Task<T> LoadAsync<T>(string id, Action<IIncludeBuilder<T>> includes, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Loads the specified entities with the specified ids, 
        ///     and includes other Documents and/or Counters.
        /// </summary>
        /// <param name="includes">
        ///     An action that specifies which documents and\or counters 
        ///     to include, by using the IIncludeBuilder interface.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        Task<Dictionary<string, T>> LoadAsync<T>(IEnumerable<string> ids, Action<IIncludeBuilder<T>> includes, CancellationToken token = default(CancellationToken));
    }
}
