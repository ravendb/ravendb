using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Voron.Impl.Journal;
using Voron.Impl.Scratch;

namespace Voron.Debugging
{
    public sealed class InMemoryStorageState
    {
        public long CurrentReadTransactionId { get; set; }

        public long PossibleOldestReadTransaction { get; set; }

        public EnvironmentStateRecordDetails EnvironmentStateRecord { get; set; }

        public List<ActiveTransaction> ActiveTransactions { get; set; }

        public List<EnvironmentStateRecordDetails> TransactionsToFlush { get; set; }

        public FlushStateDetails FlushState { get; set; }

        public SyncStateDetails SyncState { get; set; }

        public DateTime CurrentTime { get; set; } = DateTime.UtcNow;

        public sealed class FlushStateDetails
        {
            public DateTime LastFlushTime { get; set; }

            public long LastFlushedTransactionId { get; set; }

            public long LastFlushedJournalId { get; set; }

            public bool ShouldFlush { get; set; }

            public List<long> JournalsToDelete { get; set; }

            public int TotalCommittedSinceLastFlushPages { get; set; }
        }

        public sealed class SyncStateDetails
        {
            public DateTime LastSyncTime { get; set; }

            public long TotalWrittenButUnsyncedBytes { get; set; }

            public bool ShouldSync { get; set; }

        }

        public sealed class EnvironmentStateRecordDetails
        {
            public long TransactionId { get; set; }
          
            public ScratchTableDetails ScratchPagesTable { get; set; }
            
            public long WrittenToJournalNumber { get; set; }
            
            public long NextPageNumber { get; set; }

            public string ClientStateType { get; set; }
        }

        public sealed class ScratchTableDetails
        {
            public int NumberOfPages { get; set; }

            public long MinAllocatedInTransaction { get; set; }

            public long MaxAllocatedInTransaction { get; set; }

            public int MinScratchNumber { get; set; }

            public int MaxScratchNumber { get; set; }
        }
    }

}
