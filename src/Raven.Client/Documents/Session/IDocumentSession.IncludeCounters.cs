//-----------------------------------------------------------------------
// <copyright file="IDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Session.Loaders;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Interface for document session
    /// </summary>
    public partial interface IDocumentSession
    {
        /// <summary>
        ///     Begin a load while including counter of specified name
        /// </summary>
        /// <param name="name">Name of the counter to include.</param>
        
        ILoaderWithInclude<T> IncludeCounter<T>(string name);

        /// <summary>
        ///     Begin a load while including counters of specified names
        /// </summary>
        /// <param name="names">Names of the counters to include.</param>
        
        ILoaderWithInclude<T> IncludeCounters<T>(string[] names);

        /// <summary>
        ///     Begin a load while including all the counters of the document
        /// </summary>

        ILoaderWithInclude<T> IncludeCounters<T>();
    }
}
