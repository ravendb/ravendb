using System;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Documents.Queries
{
    /// <summary>
    /// Options for waiting for indexing to complete after a patch operation.
    /// </summary>
    public sealed class IndexPatchOptions
    {
        public IndexPatchOptions()
        {
        }

        public IndexPatchOptions(TimeSpan waitForIndexesTimeout)
        {
            WaitForIndexesTimeout = waitForIndexesTimeout;
        }

        /// <summary>
        /// Maximum time to wait for the indexes to become non-stale.
        /// </summary>
        public TimeSpan WaitForIndexesTimeout { get; set; } = DocumentConventions.DefaultWaitForNonStaleResultsTimeout;
        /// <summary>
        /// if true, throw when the timeout is reached, otherwise return normally.
        /// </summary>
        public bool ThrowOnTimeoutInWaitForIndexes { get; set; }
        /// <summary>
        /// A list of index names to wait for. When not set, the server waits only for the indexes it infers from the documents matched by the query or patch. When provided,
        /// the server waits for exactly these index names.
        /// /// </summary>
        public string[] WaitForSpecificIndexes { get; set; }
    }
}
