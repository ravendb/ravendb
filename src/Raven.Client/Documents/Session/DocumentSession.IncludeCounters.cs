//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Session.Loaders;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        public ILoaderWithInclude<T> IncludeCounter<T>(string name)
        {
            return new MultiLoaderWithInclude<T>(this).IncludeCounter(name);
        }

        public ILoaderWithInclude<T> IncludeCounters<T>(string[] names)
        {
            return new MultiLoaderWithInclude<T>(this).IncludeCounters(names);
        }

        public ILoaderWithInclude<T> IncludeCounters<T>()
        {
            return new MultiLoaderWithInclude<T>(this).IncludeCounters(new string[0]);
        }
    }
}
