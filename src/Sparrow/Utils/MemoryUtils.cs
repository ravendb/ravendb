using System;
using System.Linq;
using System.Text;
using Sparrow.LowMemory;

namespace Sparrow.Utils;

public static class MemoryUtils
{
    private const string GenericOutMemoryException = "Failed to generate an out of memory exception";

    public static string GetExtendedMemoryInfo(MemoryInfoResult memoryInfo)
    {


        try
        {
            var sb = new StringBuilder();
            TryAppend(() => $"Commit charge: {memoryInfo.CurrentCommitCharge} / {memoryInfo.TotalCommittableMemory}, ");
            TryAppend(() => $"Memory: {memoryInfo.TotalPhysicalMemory - memoryInfo.AvailableMemory} / {memoryInfo.TotalPhysicalMemory}, ");
            TryAppend(() => $"Available memory for processing: {memoryInfo.AvailableMemoryForProcessing}, ");
            TryAppend(() => $"Dirty memory: {memoryInfo.TotalScratchDirtyMemory}, ");
            TryAppend(() => $"Managed memory: {new Size(AbstractLowMemoryMonitor.GetManagedMemoryInBytes(), SizeUnit.Bytes)}, ");
            TryAppend(() => $"Unmanaged allocations: {new Size(AbstractLowMemoryMonitor.GetUnmanagedAllocationsInBytes(), SizeUnit.Bytes)}");

            try
            {
                var threadsMessage = GenerateThreadsMessage();
                if (threadsMessage != null)
                    TryAppend(() => threadsMessage);
            }
            catch
            {
                // nothing we can do here
            }

            return sb.ToString();

            void TryAppend(Func<string> getMessage)
            {
                try
                {
                    sb.Append(getMessage());
                }
                catch
                {
                    // nothing we can do here
                }
            }
        }
        catch
        {
            return GenericOutMemoryException;
        }
    }

    private static string GenerateThreadsMessage()
    {
        try
        {
            var topThreadsText = new StringBuilder();
            const int minAllocatedThresholdInBytes = 10 * 1024 * 1024;
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
                    break;

                if (first == false)
                    topThreadsText.Append(", ");

                first = false;

                topThreadsText.Append($"name: {stats.Name}, allocations: {new Size(stats.Allocated, SizeUnit.Bytes)}");

                if (stats.Count > 1)
                {
                    topThreadsText.Append($" (threads count: {stats.Count})");
                }
            }

            
            return topThreadsText.Length > 0 ? $", Top unmanaged allocations: {topThreadsText}" : null;
        }
        catch
        {
            return null;
        }
    }
}
