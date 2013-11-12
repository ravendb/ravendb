using System.Collections.Generic;

namespace Voron.Impl.FreeSpace
{
    public class NoFreeSpaceHandling : IFreeSpaceHandling
    {
        public long? TryAllocateFromFreeSpace(Transaction tx, int num)
        {
            return null;
        }

        public long GetFreePageCount()
        {
            return 0;
        }

        public List<long> AllPages(Transaction tx)
        {
            return new List<long>();
        }

        public void FreePage(Transaction tx, long pageNumber)
        {
            
        }
    }
}