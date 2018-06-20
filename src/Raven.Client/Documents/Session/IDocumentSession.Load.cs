//-----------------------------------------------------------------------
// <copyright file="IDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Documents.Session.Loaders;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Interface for document session
    /// </summary>
    public partial interface IDocumentSession
    {
        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        T Load<T>(string id);

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded.</param>
        Dictionary<string, T> Load<T>(IEnumerable<string> ids);

        /// <summary>
        ///     Loads the specified entity with the specified id, 
        ///     and includes other Documents and/or Counters.
        /// </summary>
        /// <param name="includes">
        ///     An action that specifies which documents and\or counters 
        ///     to include, by using the IIncludeBuilder interface.
        /// </param>
        T Load<T>(string id, Action<IIncludeBuilder<T>> includes);

        /// <summary>
        ///     Loads the specified entities with the specified ids, 
        ///     and includes other Documents and/or Counters.
        /// </summary>
        /// <param name="includes">
        ///     An action that specifies which documents and\or counters 
        ///     to include, by using the IIncludeBuilder interface.
        /// </param>
        Dictionary<string, T> Load<T>(IEnumerable<string> ids, Action<IIncludeBuilder<T>> includes);

    }
}
