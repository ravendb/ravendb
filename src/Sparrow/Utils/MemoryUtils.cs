using System.Linq;
using Sparrow.LowMemory;

namespace Sparrow.Utils;

public static class MemoryUtils
{
    public static string GetExtendedMemoryInfo(MemoryInfoResult memoryInfo)
    {
        var message =
            $"Commit charge: {memoryInfo.CurrentCommitCharge} / {memoryInfo.TotalCommittableMemory}, " +
            $"Memory: {memoryInfo.TotalPhysicalMemory - memoryInfo.AvailableMemory} / {memoryInfo.TotalPhysicalMemory}, " +
            $"Available memory for processing: {memoryInfo.AvailableMemoryForProcessing}, " +
            $"Dirty memory: {memoryInfo.TotalScratchDirtyMemory}, " +
            $"Managed memory: {new Size(AbstractLowMemoryMonitor.GetManagedMemoryInBytes(), SizeUnit.Bytes)}, " +
            $"Unmanaged allocations: {new Size(AbstractLowMemoryMonitor.GetUnmanagedAllocationsInBytes(), SizeUnit.Bytes)}";

        var topThreadsText = string.Empty;
        const int minAllocatedThresholdInBytes = 10 * 1024 * 1024;
        var numberOfLoggedUnmanagedThreads = 0;
        var first = true;

        foreach (var stats in NativeMemory.AllThreadStats
                     .Where(x => x.IsThreadAlive())
                     .GroupBy(x => x.Name)
                     .Select(x => new
                     {
                         Name = x.Key,
                         Allocated = x.Sum(y => y.TotalAllocated),
                         Count = x.Count()
                     })
                     .OrderByDescending(x => x.Allocated)
                     .Take(5))
        {
            if (stats.Allocated < minAllocatedThresholdInBytes)
                continue;
            
            if (first == false)
                topThreadsText += ", ";

            first = false;

            topThreadsText += $"name: {stats.Name}, allocations: {new Size(stats.Allocated, SizeUnit.Bytes)}";

            if (stats.Count > 1)
            {
                topThreadsText += $" (threads count: {stats.Count})";
            }

            numberOfLoggedUnmanagedThreads++;
        }

        if (numberOfLoggedUnmanagedThreads > 0)
        {
            message += $", Top {numberOfLoggedUnmanagedThreads} unmanaged allocations: {topThreadsText}";
        }

        return message;
    }
}
