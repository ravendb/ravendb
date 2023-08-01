using System.Collections.Generic;

namespace Voron.Debugging
{
    public sealed class EnvironmentStats
    {
        public long FreePagesOverhead;
        public long RootPages;
        public long UnallocatedPagesAtEndOfFile;
        public long UsedDataFileSizeInBytes;
        public long AllocatedDataFileSizeInBytes;
        public long NextWriteTransactionId;
    }

    public sealed class ActiveTransaction
    {
        public long Id;
        public TransactionFlags Flags;
        public bool AsyncCommit;

        public override string ToString()
        {
            return $"{nameof(Id)}: {Id}: {Flags} ({nameof(AsyncCommit)}: {AsyncCommit})";
        }
    }
}
