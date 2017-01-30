//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Indexes;

namespace Raven.NewClient.Client.Document
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface ISyncAdvancedSessionOperation
    {
        /// <summary>
        /// Returns all previous document revisions for specified document (with paging).
        /// </summary>
        List<T> GetRevisionsFor<T>(string id, int start = 0, int pageSize = 25);
    }
}
