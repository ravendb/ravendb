using System;
using System.Collections.Generic;

namespace Voron.Impl.Scratch
{
    public class ScratchBufferPoolInfo
    {
        public ScratchBufferPoolInfo()
        {
            ScratchFilesUsage = new List<ScratchFileUsage>();
        }

        public long OldestActiveTransaction { get; set; }

        public int NumberOfScratchFiles { get; set; }

        public int CurrentFileNumber { get; set; }

        public long CurrentFileSizeInMB { get; set; }

        public long PerScratchFileSizeLimitInMB { get; set; }

        public List<ScratchFileUsage> ScratchFilesUsage { get; set; }

        public DateTime CurrentUtcTime { get; set; }
    }

    public class MostAvailableFreePagesBySize
    {
        public long Size { get; set; }
        public long ValidAfterTransactionId { get; set; }
    }

    public class AllocatedPageInScratchBuffer
    {
        public int ScratchFileNumber { get; set; }
        public long PositionInScratchBuffer { get; set; }
        public long Size { get; set; }
        public int NumberOfPages { get; set; }
        public long ScratchPageNumber { get; set; }
    }

    public class ScratchFileUsage
    {
        public ScratchFileUsage()
        {
            MostAvailableFreePages = new List<MostAvailableFreePagesBySize>();
            First10AllocatedPages = new List<AllocatedPageInScratchBuffer>();
        }

        public string Name { get; set; }

        public long SizeInKB { get; set; }

        public int NumberOfAllocations { get; set; }

        public long AllocatedPagesCount { get; set; }

        public long TxIdAfterWhichLatestFreePagesBecomeAvailable { get; set; }

        public bool CanBeDeleted { get; set; }

        public List<MostAvailableFreePagesBySize> MostAvailableFreePages { get; set; }

        public List<AllocatedPageInScratchBuffer> First10AllocatedPages { get; set; }

        public bool IsInRecycleArea { get; set; }

        public DateTime? LastResetTime { get; set; }

        public int NumberOfResets { get; set; }

        public DateTime? LastFreeTime { get; set; }
    }
}
