namespace Sparrow.LowMemory
{
    public struct MemoryInfoResult
    {
        internal sealed class MemoryUsageIntervals
        {
            public Size LastOneMinute;
            public Size LastFiveMinutes;
            public Size SinceStartup;
        }

        internal sealed class MemoryUsageLowHigh
        {
            public MemoryUsageIntervals High;
            public MemoryUsageIntervals Low;
        }

        public string Remarks;
        public Size TotalCommittableMemory;
        public Size CurrentCommitCharge;

        public Size TotalPhysicalMemory;
        public Size InstalledMemory;
        public Size WorkingSet;
        public Size AvailableMemory;
        public Size AvailableMemoryForProcessing;
        public Size SharedCleanMemory;
        public Size TotalScratchDirtyMemory;

        public Size TotalSwapSize;
        public Size TotalSwapUsage;
        public Size WorkingSetSwapUsage;

        public bool IsExtended;
    }
}
