//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Indexes
{
    public class PutIndexResult
    {
        /// <summary>
        /// Gets or sets the total results for this query
        /// </summary>
        public string Index { get; set; }

        public long RaftCommandIndex { get; set; }
    }
}
