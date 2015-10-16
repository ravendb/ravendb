using System.Collections.Generic;

namespace Voron.Impl.FreeSpace
{
    public class NoFreeSpaceHandling : IFreeSpaceHandling
    {
        public long? TryAllocateFromFreeSpace(LowLevelTransaction tx, int num)
        {
            return null;
        }

        public List<long> AllPages(LowLevelTransaction tx)
        {
            return new List<long>();
        }

        public void FreePage(LowLevelTransaction tx, long pageNumber)
        {
            
        }
    }
}
