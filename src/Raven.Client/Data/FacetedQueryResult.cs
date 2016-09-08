using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Client.Data
{
    public class FacetedQueryResult
    {
        public static FacetedQueryResult NotModifiedResult = new FacetedQueryResult { NotModified = true };

        /// <summary>
        /// A list of results for the facet.  One entry for each term/range as specified in the facet setup document.
        /// </summary>
        public Dictionary<string, FacetResult> Results { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the index is stale.
        /// <para>Value:</para>
        /// <para>- <c>true</c> - if index is stale</para>
        /// <para>- <c>false</c> - otherwise</para>
        /// </summary>
        /// <value><c>true</c> if the index is stale; otherwise, <c>false</c>.</value>
        public bool IsStale { get; set; }

        /// <summary>
        /// The ETag value for this index current state, which include what docs were indexed,
        /// what document were deleted, etc.
        /// </summary>
        public long ResultEtag { get; set; }

        public bool NotModified { get; private set; }

        public DateTime LastQueryTime { get; set; }

        public DateTime IndexTimestamp { get; set; }

        public string IndexName { get; set; }

        public FacetedQueryResult()
        {
            Results = new Dictionary<string, FacetResult>();
        }
    }
}
