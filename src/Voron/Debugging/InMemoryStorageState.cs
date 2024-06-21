using System;
using System.Collections.Generic;

namespace Voron.Debugging
{
    public sealed class InMemoryStorageState
    {
        public long CurrentReadTransactionId { get; set; }

        public long PossibleOldestReadTransaction { get; set; }

        public List<ActiveTransaction> ActiveTransactions { get; set; }

        public FlushStateDetails FlushState { get; set; }

        public SyncStateDetails SyncState { get; set; }


        public sealed class FlushStateDetails
        {
            public DateTime LastFlushTime { get; set; }

            public long LastFlushedTransactionId { get; set; }

            public long LastFlushedJournalId { get; set; }

            public bool ShouldFlush { get; set; }

            public List<long> JournalsToDelete { get; set; }
        }

        public sealed class SyncStateDetails
        {
            public DateTime LastSyncTime { get; set; }

            public long TotalWrittenButUnsyncedBytes { get; set; }

            public bool ShouldSync { get; set; }

        }
    }

}
