//-----------------------------------------------------------------------
// <copyright file="ISessionDocumentCounters.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Counters advanced synchronous session operations
    /// </summary>
    public interface ISessionDocumentCounters : ISessionDocumentCountersBase
    {
        /// <summary>
        /// Returns all the counters for a document.
        /// </summary>
        Dictionary<string, long?> GetAll();

        /// <summary>
        /// Returns the counter by the  counter name.
        /// </summary>
        long? Get(string counter);

        /// <summary>
        /// Returns the a dictionary of counter values by counter names
        /// <param name="counters">counters names</param>
        /// </summary>
        Dictionary<string, long?> Get(IEnumerable<string> counters);

    }
}
