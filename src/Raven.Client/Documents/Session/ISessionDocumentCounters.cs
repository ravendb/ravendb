//-----------------------------------------------------------------------
// <copyright file="ISessionDocumentCounters.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Provides synchronous client API for counter operations on a specific entity
    /// </summary>
    public interface ISessionDocumentCounters : ISessionDocumentCountersBase
    {
        /// <inheritdoc cref="IAsyncSessionDocumentCounters.GetAllAsync"/> 
        Dictionary<string, long?> GetAll();

        /// <inheritdoc cref="IAsyncSessionDocumentCounters.GetAsync"/> 
        long? Get(string counter);

        /// <summary>
        /// Get values of multiple counters of the same document
        /// </summary>
        /// <param name="counters">Names of the counters to get</param>
        /// <returns>A dictionary of counter values by counter names</returns>
        Dictionary<string, long?> Get(IEnumerable<string> counters);

    }
}
