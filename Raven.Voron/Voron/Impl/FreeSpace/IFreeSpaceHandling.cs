using System.Collections.Generic;

namespace Voron.Impl.FreeSpace
{
    public interface IFreeSpaceHandling
    {
        long? TryAllocateFromFreeSpace(Transaction tx, int num);
        List<long> AllPages(Transaction tx);
        void FreePage(Transaction tx, long pageNumber);
        FreeSpaceHandlingDisabler Disable();
    }
}
