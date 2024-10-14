//-----------------------------------------------------------------------
// <copyright file="RevisionsConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.Revisions
{
    /// <summary>
    /// Represents the result of a GetRevisionsOperation, containing a list of revision objects and the total count of results.
    /// </summary>
    /// <typeparam name="T">The type of the document for which the revisions are being retrieved (contained in the results).</typeparam>
    public sealed class RevisionsResult<T>
    {
        /// <summary>
        ///The list of revisions.
        /// </summary>
        public List<T> Results { get; set; }

        /// <summary>
        /// Total number of revisions the document has.
        /// </summary>
        public int TotalResults { get; set; }
    }
}
