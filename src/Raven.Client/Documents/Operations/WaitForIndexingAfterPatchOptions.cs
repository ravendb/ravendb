using System;

namespace Raven.Client.Documents.Operations
{
    /// <summary>Options for waiting for indexing to complete after a patch operation.</summary>
    public sealed class WaitForIndexingAfterPatchOptions
    {
        public TimeSpan? WaitForIndexesTimeout { get; set; }
        public bool ThrowOnTimeoutInWaitForIndexes { get; set; }
        public string[] WaitForSpecificIndexes { get; set; }
    }
}
