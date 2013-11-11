using System.Collections.Generic;
using Voron.Trees;

namespace Voron.Impl
{
    public interface IFreeSpaceRepository
    {
        long? TryAllocateFromFreeSpace(Transaction tx, int num);
        long GetFreePageCount();
        void FlushFreeState(Transaction transaction);
        void LastTransactionPageUsage(int pages);
        List<long> AllPages(Transaction tx);
        void RegisterFreePages(Slice slice, long id, List<long> freedPages);
        void UpdateSections(Transaction tx, long oldestTransaction);
        int MinimumFreePagesInSection { get; set; }
    }

    public class NoFreeSpaceRepository : IFreeSpaceRepository
    {
        public long? TryAllocateFromFreeSpace(Transaction tx, int num)
        {
            return null;
        }

        public long GetFreePageCount()
        {
            return 0;
        }

        public void FlushFreeState(Transaction transaction)
        {
        }

        public void LastTransactionPageUsage(int pages)
        {
        }

        public List<long> AllPages(Transaction tx)
        {
            return new List<long>();
        }

        public void RegisterFreePages(Slice slice, long id, List<long> freedPages)
        {
        }

        public void UpdateSections(Transaction tx, long oldestTransaction)
        {
        }

        public int MinimumFreePagesInSection { get; set; }
    }
}