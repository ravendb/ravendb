using System;
using System.Collections.Generic;

namespace Voron.Debugging
{
    public class InMemoryStorageState
    {
        public long CurrentReadTransactionId { get; set; }

        public long NextWriteTransactionId { get; set; }

        public long PossibleOldestReadTransaction { get; set; }

        public List<ActiveTransaction> ActiveTransactions { get; set; }

        public FlushStateDetails FlushState { get; set; }

        public SyncStateDetails SyncState { get; set; }


        public class FlushStateDetails
        {
            public DateTime LastFlushTime { get; set; }

            public long LastFlushedTransactionId { get; set; }

            public long LastFlushedJournalId { get; set; }

            public long LastTransactionIdUsedToReleaseScratches { get; set; }

            public bool ShouldFlush { get; set; }

            public List<long> JournalsToDelete { get; set; }
        }

        public class SyncStateDetails
        {
            public DateTime LastSyncTime { get; set; }

            public long TotalWrittenButUnsyncedBytes { get; set; }

            public bool ShouldSync { get; set; }

        }
    }

}
