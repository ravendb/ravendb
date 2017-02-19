using System.Collections.Generic;

namespace Voron.Impl.Scratch
{
    public class ScratchBufferPoolInfo
    {
        public ScratchBufferPoolInfo()
        {
            ScratchFilesUsage = new List<ScratchFileUsage>();
            MostAvailableFreePages = new List<MostAvailableFreePagesByScratch>();
        }

        public long TxIdAfterWhichLatestFreePagesBecomeAvailable { get; set; }

        public int NumberOfScratchFiles { get; set; }

        public long CurrentFileSizeInMB { get; set; }

        public long PerScratchFileSizeLimitInMB { get; set; }

        public long TotalScratchFileSizeLimitInMB { get; set; }

        public List<ScratchFileUsage> ScratchFilesUsage { get; set; }

        public List<MostAvailableFreePagesByScratch> MostAvailableFreePages { get; set; }
    }

    public class MostAvailableFreePagesByScratch
    {
        public MostAvailableFreePagesByScratch()
        {
            MostAvailableFreePages = new List<MostAvailableFreePagesBySize>();
        }

        public string Name { get; set; }

        public List<MostAvailableFreePagesBySize> MostAvailableFreePages { get; set; }
    }

    public class MostAvailableFreePagesBySize
    {
        public long Size { get; set; }
        public long ValidAfterTransactionId { get; set; }
    }

    public class ScratchFileUsage
    {
        public string Name { get; set; }

        public long SizeInKB { get; set; }

        public long InActiveUseInKB { get; set; }

        public long TxIdAfterWhichLatestFreePagesBecomeAvailable { get; set; }
    }
}