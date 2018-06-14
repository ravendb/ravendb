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
    ///     Advanced async counters session operations
    /// </summary>
    public interface IAsyncSessionDocumentCounters : ISessionDocumentCountersBase
    {
        /// <summary>
        /// Returns all the counters for a specific document.
        /// </summary>
        Task<Dictionary<string, long?>> GetAllAsync(CancellationToken token = default);

        /// <summary>
        /// Returns the counter value by counter name.
        /// </summary>
        Task<long?> GetAsync(string counter, CancellationToken token = default);

        /// <summary>
        /// Returns the a dictionary of counter values by counter names
        /// <param name="counters">counters names</param>
        /// </summary>
        Task<Dictionary<string, long?>> GetAsync(IEnumerable<string> counters, CancellationToken token = default);

    }
}
