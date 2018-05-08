//-----------------------------------------------------------------------
// <copyright file="ICountersSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Client.Documents.Operations.Counters;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Counters advanced synchronous session operations
    /// </summary>
    public interface ICountersSessionOperations : ICountersSessionOperationsBase
    {
        /// <summary>
        /// Returns all the counters for a specific document.
        /// </summary>
        Dictionary<string, long> Get(string documentId);

        /// <summary>
        /// Returns all the counters for an entity.
        /// </summary>
        Dictionary<string, long> Get(object entity);

        /// <summary>
        /// Returns the counter by the document id and counter name.
        /// </summary>
        long? Get(string documentId, string counter);

        /// <summary>
        /// Returns the counter by entity and counter name.
        /// </summary>
        long? Get(object entity, string counter);

        /// <summary>
        /// Returns CountersDetail on all the specified counters, by document id and counter names
        /// <param name="documentId">the document which holds the counters</param>
        /// <param name="counters">counters names</param>
        /// </summary>
        Dictionary<string, long> Get(string documentId, IEnumerable<string> counters);

        /// <summary>
        /// Returns CountersDetail on all the specified counters
        /// <param name="entity">instance of entity of the document which holds the counter</param>
        /// <param name="counters">counters names</param>
        /// </summary>
        Dictionary<string, long> Get(object entity, IEnumerable<string> counters);

    }
}
