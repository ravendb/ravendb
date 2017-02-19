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

        public long CurrentFileSizeInMB { get; set; }

        public long PerScratchFileSizeLimitInMB { get; set; }

        public long TotalScratchFileSizeLimitInMB { get; set; }

        public List<ScratchFileUsage> ScratchFilesUsage { get; set; }
    }

    public class MostAvailableFreePagesBySize
    {
        public long Size { get; set; }
        public long ValidAfterTransactionId { get; set; }
    }

    public class ScratchFileUsage
    {
        public ScratchFileUsage()
        {
            MostAvailableFreePages = new List<MostAvailableFreePagesBySize>();
        }

        public string Name { get; set; }

        public long SizeInKB { get; set; }

        public int NumberOfAllocations { get; set; }

        public long InActiveUseInKB { get; set; }

        public long TxIdAfterWhichLatestFreePagesBecomeAvailable { get; set; }

        public bool CanBeDeleted { get; set; }

        public List<MostAvailableFreePagesBySize> MostAvailableFreePages { get; set; }
    }
}