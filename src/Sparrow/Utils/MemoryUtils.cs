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
            TryAppend(() => sb.Append("Commit charge: ").Append(memoryInfo.CurrentCommitCharge).Append(" / ").Append(memoryInfo.TotalCommittableMemory).Append(", "));
            TryAppend(() => sb.Append("Memory: ").Append(memoryInfo.TotalPhysicalMemory - memoryInfo.AvailableMemory).Append(" / ").Append(memoryInfo.TotalPhysicalMemory).Append(", "));
            TryAppend(() => sb.Append("Available memory for processing: ").Append(memoryInfo.AvailableMemoryForProcessing).Append(", "));
            TryAppend(() => sb.Append("Dirty memory: ").Append(memoryInfo.TotalScratchDirtyMemory).Append(", "));
            TryAppend(() => sb.Append("Managed memory: ").Append(new Size(AbstractLowMemoryMonitor.GetManagedMemoryInBytes(), SizeUnit.Bytes)).Append(", "));
            TryAppend(() => sb.Append("Unmanaged allocations: ").Append(new Size(AbstractLowMemoryMonitor.GetUnmanagedAllocationsInBytes(), SizeUnit.Bytes)));

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

                    TryAppend(() => sb.Append("name: ").Append(keyValue.Value).Append(", allocations: ").Append(new Size(keyValue.Key, SizeUnit.Bytes)));
                    if (keyValue.Value == null)
                        TryAppend(() => sb.Append(" (threads count: ").Append(unknownThreadsCount).Append(")"));
                }
            }
            catch
            {
                // nothing we can do here
            }

            return sb.ToString();

            void TryAppend(Action append)
            {
                try
                {
                    append();
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
