using System.Collections.Generic;

namespace Voron.Debugging
{
    public class EnvironmentStats
    {
        public long FreePagesOverhead;
        public long RootPages;
        public long NumberOfAllocatedPages;
        public long NextPageNumber;
        public long UnallocatedPagesAtEndOfFile;
        public long UsedDataFileSizeInBytes;
        public long AllocatedDataFileSizeInBytes;
        public long NextWriteTransactionId;
        public List<ActiveTransaction> ActiveTransactions;
    }

    public class ActiveTransaction
    {
        public long Id;
        public TransactionFlags Flags;
    }
}
