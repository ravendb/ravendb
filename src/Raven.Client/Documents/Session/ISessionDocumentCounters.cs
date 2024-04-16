//-----------------------------------------------------------------------
// <copyright file="ISessionDocumentCounters.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Provides client API for counter operations on a specific entity.<br/>
    /// Counters are numeric data variables that can be added to documents. <br/>
    /// They are designed to perform high frequency counting in a distributed manner, <br/>
    /// while ensuring conflict-free behavior.
    /// </summary>
    /// <inheritdoc cref="DocumentationUrls.Session.Counters.Overview"/>
    public interface ISessionDocumentCounters : ISessionDocumentCountersBase
    {
        /// <summary>
        /// Get all counters for a specific document.
        /// </summary>
        ///<returns>A Dictionary of counter values by counter name, containing all counters for this document</returns>
        Dictionary<string, long?> GetAll();

        /// <summary>
        /// Get counter value by counter name.
        /// </summary>
        ///<param name="counter">Name of the counter to get</param>
        /// <returns>The counter value if exists, or Null if the counter does not exist</returns>
        long? Get(string counter);

        /// <summary>
        /// Get values of multiple counters of the same document
        /// </summary>
        /// <param name="counters">Names of the counters to get</param>
        /// <returns>A dictionary of counter values by counter names</returns>
        Dictionary<string, long?> Get(IEnumerable<string> counters);

    }
}
