using System;

namespace Sparrow.LowMemory
{
    public struct MemoryInfoResult
    {
        public string Remarks;
        public Size TotalCommittableMemory;
        public Size CurrentCommitCharge;

        public Size TotalPhysicalMemory;
        public Size InstalledMemory;
        public Size WorkingSet;
        public Size AvailableMemory;
        public Size AvailableMemoryForProcessing;
        public Size SharedCleanMemory;

        public Size TotalSwapSize;
        public Size TotalSwapUsage;
        public Size WorkingSetSwapUsage;

        public bool IsExtended;
    }
}
