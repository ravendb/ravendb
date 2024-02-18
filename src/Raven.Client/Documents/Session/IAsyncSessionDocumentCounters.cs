//-----------------------------------------------------------------------
// <copyright file="IAsyncSessionDocumentCounters.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Provides async client API for counter operations on a specific entity
    /// </summary>
    public interface IAsyncSessionDocumentCounters : ISessionDocumentCountersBase
    {
        /// <summary>
        /// Get all counters for a specific document.
        /// </summary>
        ///<returns>A Dictionary of counter values by counter name, containing all counters for this document</returns>
        Task<Dictionary<string, long?>> GetAllAsync(CancellationToken token = default);

        /// <summary>
        /// Get counter value by counter name.
        /// </summary>
        ///<param name="counter">Name of the counter to get</param>
        /// <returns>The counter value if exists, or Null if the counter does not exist</returns>
        Task<long?> GetAsync(string counter, CancellationToken token = default);

        /// <summary>
        /// Get values of multiple counters of the same document
        /// </summary>
        /// <param name="counters">Names of the counters to get</param>
        /// <returns>A dictionary of counter values by counter names</returns>
        Task<Dictionary<string, long?>> GetAsync(IEnumerable<string> counters, CancellationToken token = default);

    }
}
