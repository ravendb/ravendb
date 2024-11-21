using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Binary;
using Sparrow.Global;
using Sparrow.Platform;

namespace Sparrow.Server.Platform;

public static unsafe class MemoryLockUsage
{
        public static long LockedMemorySize;
        
        public static event EventHandler<long> MemoryLockedCalled;

        public static void UpdateLockedMemory(Int64 v, char* filenamePtr)
        {
            MemoryLockedCalled?.Invoke(null, v);
            Interlocked.Add(ref LockedMemorySize, v);
        }
        
        public static bool RecoverLockedMemoryFailure(Int64 sizeToLock, char* filenamePtr)
        {
            if (PlatformDetails.RunningOnPosix)
                return false; // nothing to do here
            using var currentProcess = Process.GetCurrentProcess();
            var nextWorkingSetSize = GetNearestFileSize(currentProcess.MinWorkingSet.ToInt64() + sizeToLock);
            if (nextWorkingSetSize > int.MaxValue && PlatformDetails.Is32Bits)
            {
                nextWorkingSetSize = int.MaxValue;
            }
            
#pragma warning disable CA1416 // Validate platform compatibility
            // Minimum working set size must be less than or equal to the maximum working set size.
            // Let's increase the max as well.
            if (nextWorkingSetSize > currentProcess.MaxWorkingSet)
            {
                try
                {
                    currentProcess.MaxWorkingSet = new IntPtr(nextWorkingSetSize);
                }
                catch
                {
                    // cannot throw, called from native code
                    return false;
                }
            }
            
            try
            {
                currentProcess.MinWorkingSet = new IntPtr(nextWorkingSetSize);
            }
            catch
            {
                // cannot throw, called from native code
                return false;
            }
#pragma warning restore CA1416 // Validate platform compatibility

            return true;
        }
        
        private static readonly long IncreaseByPowerOf2Threshold = new Size(512, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes);
        private static long GetNearestFileSize(long neededSize)
        {
            if (neededSize < IncreaseByPowerOf2Threshold)
                return Bits.PowerOf2(neededSize);

            // if it is over 0.5 GB, then we grow at 1 GB intervals
            var remainder = neededSize % Constants.Size.Gigabyte;
            if (remainder == 0)
                return neededSize;

            // above 0.5GB we need to round to the next GB number
            return neededSize + Constants.Size.Gigabyte - remainder;
        }
        
}
