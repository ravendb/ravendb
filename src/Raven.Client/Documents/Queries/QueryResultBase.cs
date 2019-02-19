using System;
using System.Collections.Generic;
using Raven.Client.Documents.Queries.Timings;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries
{
    public abstract class QueryResultBase<TResult, TInclude>
    {
        /// <summary>
        /// Gets or sets the document resulting from this query.
        /// </summary>
        public TResult Results { get; set; }

        /// <summary>
        /// Gets or sets the document included in the result.
        /// </summary>
        public TInclude Includes { get; set; }

        /// <summary>
        /// Gets or sets the Counters included in the result.
        /// </summary>
        public BlittableJsonReaderObject CounterIncludes { get; set; }

        /// <summary>
        /// The names of all the counters that the server
        /// was asked to include in the result, by document id.
        /// </summary>
        public Dictionary<string, string[]> IncludedCounterNames { get; set; }

        /// <summary>
        /// The paths that the server included in the results
        /// </summary>
        public string [] IncludedPaths { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the index is stale.
        /// <para>Value:</para>
        /// <para>- <c>true</c> - if index is stale</para>
        /// <para>- <c>false</c> - otherwise</para>
        /// </summary>
        /// <value><c>true</c> if the index is stale; otherwise, <c>false</c>.</value>
        public bool IsStale { get; set; }

        /// <summary>
        /// The last time the index was updated.
        /// This can be used to determine the freshness of the data.
        /// </summary>
        public DateTime IndexTimestamp { get; set; }

        /// <summary>
        /// The index used to answer this query
        /// </summary>
        public string IndexName { get; set; }

        /// <summary>
        /// The ETag value for this index current state, which include what docs were indexed,
        /// what document were deleted, etc.
        /// </summary>
        public long ResultEtag { get; set; }

        /// <summary>
        /// The timestamp of the last time the index was queried
        /// </summary>
        public DateTime LastQueryTime { get; set; }

        /// <summary>
        /// Tag of a cluster node which responded to the query
        /// </summary>
        public string NodeTag { get; set; }

        /// <summary>
        /// Detailed timings for various parts of a query (Lucene search, loading documents, transforming results) - if requested.
        /// </summary>
        public QueryTimings Timings { get; set; }
    }
}
