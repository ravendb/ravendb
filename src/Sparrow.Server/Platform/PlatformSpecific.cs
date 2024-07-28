using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Global;
using Sparrow.Platform;
using Sparrow.Server.LowMemory;
using Sparrow.Server.Platform.Posix;
using Sparrow.Server.Platform.Win32;

namespace Sparrow.Server.Platform
{
    public static unsafe class PlatformSpecific
    {
        public static class MemoryInformation
        {
            public static string IsSwappingOnHddInsteadOfSsd()
            {
                if (PlatformDetails.RunningOnPosix)
                    return CheckPageFileOnHdd.PosixIsSwappingOnHddInsteadOfSsd();
                return CheckPageFileOnHdd.WindowsIsSwappingOnHddInsteadOfSsd();
            }
        }

        public static class NativeMemory
        {
            public static byte* Allocate4KbAlignedMemory(long size, out Sparrow.Utils.NativeMemory.ThreadStats thread)
            {
                Debug.Assert(size >= 0);

                thread = Sparrow.Utils.NativeMemory.ThreadAllocations.Value;
                thread.Allocations += size;

                Interlocked.Add(ref Sparrow.Utils.NativeMemory._totalAllocatedMemory, size);

                var rc = Pal.rvn_mmap_anonymous(out void* ptr, (ulong)size, out var errorCode);

                if (rc != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(rc, errorCode, "Could not allocate memory");

                return (byte*)ptr;
            }

            public static void Free4KbAlignedMemory(byte* ptr, long size, Sparrow.Utils.NativeMemory.ThreadStats stats)
            {
                Debug.Assert(ptr != null);

                if (stats != null)
                    Sparrow.Utils.NativeMemory.UpdateMemoryStatsForThread(stats, size);

                Interlocked.Add(ref Sparrow.Utils.NativeMemory._totalAllocatedMemory, -size);
                
                
                var rc = Pal.rvn_mumap_anonymous(ptr, (ulong)size, out var errorCode);

                if (rc != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(rc, errorCode, "Could not free memory");
            }

            [DoesNotReturn]
            private static void ThrowFailedToAllocate()
            {
                throw new Win32Exception("Could not allocate memory");
            }

            [DoesNotReturn]
            private static void ThrowFailedToFree()
            {
                throw new Win32Exception("Failed to free memory");
            }
        }
    }
}
