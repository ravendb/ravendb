using System.Collections.Generic;
using Voron.Trees;

namespace Voron.Impl
{
    public interface IFreeSpaceRepository
    {
        Page TryAllocateFromFreeSpace(Transaction tx, int num);
        long GetFreePageCount();
        void FlushFreeState(Transaction transaction);
        void LastTransactionPageUsage(int pages);
        List<long> AllPages(Transaction tx);
        void RegisterFreePages(Slice slice, long id, List<long> freedPages);
        void UpdateSections(Transaction tx, long oldestTransaction);
        int MinimumFreePagesInSection { get; set; }
    }
}