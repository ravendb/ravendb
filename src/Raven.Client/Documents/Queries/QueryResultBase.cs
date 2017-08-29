using System;

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
    }
}
