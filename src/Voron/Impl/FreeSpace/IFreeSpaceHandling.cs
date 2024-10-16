using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Voron.Impl.FreeSpace
{
    public interface IFreeSpaceHandling
    {
        long? TryAllocateFromFreeSpace(LowLevelTransaction tx, int num);
        List<long> AllPages(LowLevelTransaction tx);
        int GetFreePagesCount(LowLevelTransaction txLowLevelTransaction);
        List<DynamicJsonValue> FreeSpaceSnapshot(LowLevelTransaction tx, bool hex);
        IEnumerable<long> GetAllFullyEmptySegments(LowLevelTransaction tx);
        void FreePage(LowLevelTransaction tx, long pageNumber);
        long GetFreePagesOverhead(LowLevelTransaction tx);
        IEnumerable<long> GetFreePagesOverheadPages(LowLevelTransaction tx);
        FreeSpaceHandlingDisabler Disable();
    }
}
