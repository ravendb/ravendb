//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.NewClient.Client.Data.Indexes
{
    public class PutIndexResult
    {
        /// <summary>
        /// Gets or sets the total results for this query
        /// </summary>
        public string Index { get; set; }

        /// <summary>
        /// Gets or sets the total results for this query
        /// </summary>
        public int IndexId { get; set; }
    }
}
