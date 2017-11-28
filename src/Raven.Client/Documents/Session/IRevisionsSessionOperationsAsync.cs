//-----------------------------------------------------------------------
// <copyright file="IRevisionsSessionOperationsAsync.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Revisions advanced async session operations
    /// </summary>
    public interface IRevisionsSessionOperationsAsync
    {
        /// <summary>
        /// Returns all previous document revisions for specified document (with paging).
        /// </summary>
        Task<List<T>> GetForAsync<T>(string id, int start = 0, int pageSize = 25);
    }
}
