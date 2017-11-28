//-----------------------------------------------------------------------
// <copyright file="IRevisionsSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Revisions advanced synchronous session operations
    /// </summary>
    public interface IRevisionsSessionOperations
    {
        /// <summary>
        /// Returns all previous document revisions for specified document (with paging) ordered by most recent reivions first.
        /// </summary>
        List<T> GetFor<T>(string id, int start = 0, int pageSize = 25);
    }
}
