using System;
using System.Collections.Generic;
using System.Text;
using Sparrow.LowMemory;

namespace Sparrow.Utils;

public static class MemoryUtils
{
    private const string GenericOutMemoryException = "Failed to generate an out of memory exception";
    private static readonly InvertedComparer InvertedComparerInstance = new InvertedComparer();
    private const int MinAllocatedThresholdInBytes = 10 * 1024 * 1024;

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
                var sorted = new SortedDictionary<long, string>(InvertedComparerInstance);

                long totalAllocatedForUnknownThreads = 0;
                var unknownThreadsCount = 0;
                foreach (var stats in NativeMemory.AllThreadStats)
                {
                    if (stats.Name == null)
                    {
                        totalAllocatedForUnknownThreads += stats.TotalAllocated;
                        unknownThreadsCount++;
                        continue;
                    }

                    sorted[stats.TotalAllocated] = stats.Name;
                }

                sorted[totalAllocatedForUnknownThreads] = null;

                var count = 0;
                var first = true;
                foreach (var keyValue in sorted)
                {
                    if (keyValue.Key < MinAllocatedThresholdInBytes)
                        break;

                    if (++count > 5)
                        break;
                    
                    if (first)
                    {
                        sb.Append(", Top unmanaged allocations: ");
                        first = false;
                    }
                    else
                    {
                        sb.Append(", ");
                    }

                    TryAppend(() => $"name: {keyValue.Value}, allocations: {new Size(keyValue.Key, SizeUnit.Bytes)}");
                    if (keyValue.Value == null)
                        TryAppend(() => $" (threads count: {unknownThreadsCount})");
                }
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

    private class InvertedComparer : IComparer<long>
    {
        public int Compare(long x, long y)
        {
            return y.CompareTo(x);
        }
    }
}
