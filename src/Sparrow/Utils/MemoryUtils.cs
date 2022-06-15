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
            $"Dirty memory: {memoryInfo.TotalScratchDirtyMemory}, " +
            $"Managed memory: {new Size(AbstractLowMemoryMonitor.GetManagedMemoryInBytes(), SizeUnit.Bytes)}, " +
            $"Unmanaged allocations: {new Size(AbstractLowMemoryMonitor.GetUnmanagedAllocationsInBytes(), SizeUnit.Bytes)}," +
            "Top 5 unmanaged allocations: ";

        var top5 = string.Empty;
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
            if (first == false)
                top5 += ", ";

            first = false;

            top5 += $"name: {stats.Name}, allocations: {new Size(stats.Allocated, SizeUnit.Bytes)}";

            if (stats.Count > 1)
            {
                top5 += $" (threads count: {stats.Count})";
            }
        }

        return message + top5;
    }
}
