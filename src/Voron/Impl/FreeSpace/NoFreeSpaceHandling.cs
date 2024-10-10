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

        public int GetFreePagesCount(LowLevelTransaction txLowLevelTransaction)
        {
            return 0;
        }

        public void FreePage(LowLevelTransaction tx, long pageNumber)
        {
            
        }

        public long GetFreePagesOverhead(LowLevelTransaction tx)
        {
            return -1;
        }

        public IEnumerable<long> GetFreePagesOverheadPages(LowLevelTransaction tx)
        {
            yield break;
        }

        public FreeSpaceHandlingDisabler Disable()
        {
            return new FreeSpaceHandlingDisabler();
        }
    }
}
