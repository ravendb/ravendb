using System;
using System.IO.Compression;

namespace Raven.Client.Documents.BulkInsert
{
    public sealed class BulkInsertOptions
    {
        public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.NoCompression;

        /// <summary>
        /// Determines whether we should skip overwriting a document when it is updated by exactly the same document (by comparing the content and the metadata)
        /// </summary>
        public bool SkipOverwriteIfUnchanged { get; set; }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal class TestingStuff
        {
            internal Action OnSendHeartBeat_DoBulkStore;
            internal Action OnSendHeartBeat_AfterFlush;
            internal int OverrideHeartbeatCheckInterval;
        }
    }
}
