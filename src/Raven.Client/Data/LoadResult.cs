//-----------------------------------------------------------------------
// <copyright file="LoadResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

using Raven.Json.Linq;

namespace Raven.Client.Data
{
    public class LoadResult : LoadResult<RavenJObject>
    {
    }

    /// <summary>
    /// Represent a result which include both document results and included documents
    /// </summary>
    public abstract class LoadResult<T>
        where T : class, new()
    {
        /// <summary>
        /// Loaded documents. The results will be in exact same order as in keys parameter.
        /// </summary>
        public List<T> Results { get; set; }

        /// <summary>
        /// Included documents.
        /// </summary>
        public List<T> Includes { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="LoadResult"/> class.
        /// </summary>
        public LoadResult()
        {
            Results = new List<T>();
            Includes = new List<T>();
        }
    }
}
