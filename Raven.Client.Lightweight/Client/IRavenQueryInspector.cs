//-----------------------------------------------------------------------
// <copyright file="IRavenQueryInspector.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Client.Document;

namespace Raven.Client.Client
{

    /// <summary>
    /// Provide access to the underlying <see cref="IDocumentQuery{T}"/>
    /// </summary>
    internal interface IRavenQueryInspector
    {
        /// <summary>
        /// Get the name of the index being queried
        /// </summary>
        string IndexQueried { get; }

        /// <summary>
        /// Grant access to the query session
        /// </summary>
        IDocumentSession Session { get; }
        /// <summary>
        /// The last term that we asked the query to use equals on
        /// </summary>
        KeyValuePair<string, string> GetLastEqualityTerm();
    }
}
