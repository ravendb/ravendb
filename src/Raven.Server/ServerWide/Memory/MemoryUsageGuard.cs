﻿using System;
using System.Diagnostics;
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
            bool isRunningOn32Bits,
            Logger logger,
            out ProcessMemoryUsage currentUsage)
        {
            if (isRunningOn32Bits)
            {
                currentUsage = null;
                return false;
            }

            var currentlyAllocated = new Size(threadStats.TotalAllocated, SizeUnit.Bytes);

            // we run out our memory quota, so we need to see if we can increase it or break
            var memoryInfoResult = MemoryInformation.GetMemoryInfo();

            using (var currentProcess = Process.GetCurrentProcess())
            {
                // a lot of the memory that we use is actually from memory mapped files, as such, we can
                // rely on the OS to page it out (without needing to write, since it is read only in this case)
                // so we try to calculate how much such memory we can use with this assumption 
                var mappedSharedMem = LowMemoryNotification.GetCurrentProcessMemoryMappedShared();

                currentUsage = new ProcessMemoryUsage(currentProcess.WorkingSet64,
                    Math.Max(0, currentProcess.WorkingSet64 - mappedSharedMem.GetValue(SizeUnit.Bytes)));

                var memoryAssumedFreeOrCheapToFree = memoryInfoResult.AvailableMemory + mappedSharedMem;

                // there isn't enough available memory to try, we want to leave some out for other things
                if (memoryAssumedFreeOrCheapToFree < 
                    Size.Min(memoryInfoResult.TotalPhysicalMemory / 50, new Size(1, SizeUnit.Gigabytes)) )
                {
                    if (logger.IsInfoEnabled)
                    {
                        logger.Info(
                            $"{threadStats.Name} which is already using {currentlyAllocated}/{currentMaximumAllowedMemory} and the system has " +
                            $"{memoryInfoResult.AvailableMemory}/{memoryInfoResult.TotalPhysicalMemory} free RAM. Also have ~{mappedSharedMem} in mmap " +
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
                            $"{threadStats} which is already using {currentlyAllocated}/{currentMaximumAllowedMemory} and the system has" +
                            $"{memoryInfoResult.AvailableMemory}/{memoryInfoResult.TotalPhysicalMemory} free RAM. Also have ~{mappedSharedMem} in mmap " +
                            "files that can be cleanly released, not enough to proceed in batch.");
                    }
                    return false;
                }

                // even though we have twice as much memory as we have current allocated, we will 
                // only increment by 16MB to avoid over allocation by multiple indexes. This way, 
                // we'll check often as we go along this
                var oldBudget = currentMaximumAllowedMemory;
                currentMaximumAllowedMemory = currentlyAllocated + new Size(16, SizeUnit.Megabytes);

                if (logger.IsInfoEnabled)
                {
                    logger.Info(
                        $"Increasing memory budget for {threadStats.Name} which is using  {currentlyAllocated}/{oldBudget} and the system has" +
                        $"{memoryInfoResult.AvailableMemory}/{memoryInfoResult.TotalPhysicalMemory} free RAM with {mappedSharedMem} in mmap " +
                        $"files that can be cleanly released. Budget increased to {currentMaximumAllowedMemory}");
                }

                return true;
            }
        }
    }
}
