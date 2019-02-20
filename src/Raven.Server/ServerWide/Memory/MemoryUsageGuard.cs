using Sparrow;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Utils;

namespace Raven.Server.ServerWide.Memory
{
    public class MemoryUsageGuard
    {
        public static bool TryIncreasingMemoryUsageForThread(NativeMemory.ThreadStats threadStats,
            ref Size currentMaximumAllowedMemory,
            Size currentlyInUse,
            bool isRunningOn32Bits,
            Logger logger,
            out ProcessMemoryUsage currentUsage)
        {
            if (isRunningOn32Bits)
            {
                currentUsage = null;
                return false;
            }

            // we run out our memory quota, so we need to see if we can increase it or break
            var memoryInfo = MemoryInformation.GetMemoryInformationUsingOneTimeSmapsReader();
            currentUsage = GetProcessMemoryUsage(memoryInfo);

            var memoryAssumedFreeOrCheapToFree = memoryInfo.AvailableWithoutTotalCleanMemory;

            // there isn't enough available memory to try, we want to leave some out for other things
            if (memoryAssumedFreeOrCheapToFree <
                Size.Min(memoryInfo.TotalPhysicalMemory / 50, new Size(1, SizeUnit.Gigabytes)))
            {
                if (logger.IsInfoEnabled)
                {
                    logger.Info(
                        $"{threadStats.Name} which is already using {currentlyInUse}/{currentMaximumAllowedMemory} and the system has " +
                        $"{memoryInfo.AvailableWithoutTotalCleanMemory}/{memoryInfo.TotalPhysicalMemory} free RAM. Also have ~{memoryInfo.SharedCleanMemory} in mmap " +
                        "files that can be cleanly released, not enough to proceed in batch.");
                }

                return false;
            }

            // If there isn't enough here to double our current allocation, we won't allocate any more
            // we do this check in this way to prevent multiple indexes of hitting this at the
            // same time and each thinking that they have enough space
            if (memoryAssumedFreeOrCheapToFree < currentMaximumAllowedMemory)
            {
                if (logger.IsInfoEnabled)
                {
                    logger.Info(
                        $"{threadStats} which is already using {currentlyInUse}/{currentMaximumAllowedMemory} and the system has" +
                        $"{memoryInfo.AvailableWithoutTotalCleanMemory}/{memoryInfo.TotalPhysicalMemory} free RAM. Also have ~{memoryInfo.SharedCleanMemory} in mmap " +
                        "files that can be cleanly released, not enough to proceed in batch.");
                }

                return false;
            }

            var allocatedForProcessing = GetTotalCurrentlyAllocatedForProcessing();
            if (memoryAssumedFreeOrCheapToFree < allocatedForProcessing)
            {
                if (logger.IsInfoEnabled)
                {
                    logger.Info(
                        $"Total allocated memory for processing: {allocatedForProcessing}, free memory: {memoryAssumedFreeOrCheapToFree}, " +
                        $"total memory: {memoryInfo.TotalPhysicalMemory}.");
                }

                return false;
            }

            // even though we have twice as much memory as we have current allocated, we will 
            // only increment by 16MB to avoid over allocation by multiple indexes. This way, 
            // we'll check often as we go along this
            var oldBudget = currentMaximumAllowedMemory;
            currentMaximumAllowedMemory = currentlyInUse + new Size(16, SizeUnit.Megabytes);

            if (logger.IsInfoEnabled)
            {
                logger.Info(
                    $"Increasing memory budget for {threadStats.Name} which is using  {currentlyInUse}/{oldBudget} and the system has" +
                    $"{memoryAssumedFreeOrCheapToFree}/{memoryInfo.TotalPhysicalMemory} free RAM with {memoryInfo.SharedCleanMemory} in mmap " +
                    $"files that can be cleanly released. Budget increased to {currentMaximumAllowedMemory}");
            }

            return true;
        }

        public static bool CanIncreaseMemoryUsageForThread()
        {
            var memoryInfo = MemoryInformation.GetMemoryInformationUsingOneTimeSmapsReader();
            var memoryAssumedFreeOrCheapToFree = memoryInfo.AvailableWithoutTotalCleanMemory;
            var allocatedForProcessing = GetTotalCurrentlyAllocatedForProcessing();
            return memoryAssumedFreeOrCheapToFree >= allocatedForProcessing;
        }

        private static ProcessMemoryUsage GetProcessMemoryUsage(MemoryInfoResult memoryInfo)
        {
            var workingSetInBytes = memoryInfo.WorkingSet.GetValue(SizeUnit.Bytes);
            var privateMemory = AbstractLowMemoryMonitor.GetManagedMemoryInBytes() + AbstractLowMemoryMonitor.GetUnmanagedAllocationsInBytes();
            return new ProcessMemoryUsage(workingSetInBytes, privateMemory);
        }

        private static Size GetTotalCurrentlyAllocatedForProcessing()
        {
            var allocated = 0L;

            foreach (var stats in NativeMemory.AllThreadStats)
            {
                if (stats.IsThreadAlive() == false)
                    continue;

                allocated += stats.CurrentlyAllocatedForProcessing;
            }

            return new Size(allocated, SizeUnit.Bytes);
        }
    }
}
