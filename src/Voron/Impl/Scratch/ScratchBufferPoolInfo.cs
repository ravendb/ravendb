using System;
using System.Collections.Generic;

namespace Voron.Impl.Scratch
{
    public sealed class ScratchBufferPoolInfo
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

        public int MemorySegments { get; set; }

        public long MemorySegmentSizeInMB { get; set; }

        public long MemorySizeInKB { get; set; }

        public bool MemoryFailedRecently { get; set; }

        public long GlobalMemoryUsedInMB { get; set; }

        public long GlobalMemoryLimitInMB { get; set; }

        public bool GlobalMemoryUnderPressure { get; set; }
    }

    public sealed class MostAvailableFreePagesBySize
    {
        public long Size { get; set; }
        public long ValidAfterTransactionId { get; set; }

        public long AllocatedInTransaction { get; set; }
    }

    public sealed class ScratchFileUsage
    {
        public ScratchFileUsage()
        {
            MostAvailableFreePages = new List<MostAvailableFreePagesBySize>();
        }

        public string Name { get; set; }

        public bool IsMemory { get; set; }

        public bool IsDraining { get; set; }

        public long SizeInKB { get; set; }

        public int NumberOfAllocations { get; set; }

        public long AllocatedPagesCount { get; set; }

        public long TxIdAfterWhichLatestFreePagesBecomeAvailable { get; set; }

        public bool CanBeDeleted { get; set; }

        public List<MostAvailableFreePagesBySize> MostAvailableFreePages { get; set; }

        public bool IsInRecycleArea { get; set; }

        public DateTime? LastResetTime { get; set; }

        public int NumberOfResets { get; set; }

        public DateTime? LastFreeTime { get; set; }
    }
}
