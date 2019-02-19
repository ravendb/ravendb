using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Threading;
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

            public static bool WillCauseHardPageFault(byte* address, long length) => PlatformDetails.RunningOnPosix ? PosixMemoryQueryMethods.WillCauseHardPageFault(address, length) : Win32MemoryQueryMethods.WillCauseHardPageFault(address, length);
        }

        public static class NativeMemory
        {
            public static byte* Allocate4KbAlignedMemory(long size, out Sparrow.Utils.NativeMemory.ThreadStats thread)
            {
                Debug.Assert(size >= 0);

                thread = Sparrow.Utils.NativeMemory.ThreadAllocations.Value;
                thread.Allocations += size;

                Interlocked.Add(ref Sparrow.Utils.NativeMemory._totalAllocatedMemory, size);

                if (PlatformDetails.RunningOnPosix)
                {
                    byte* ptr;
                    var rc = Syscall.posix_memalign(&ptr, (IntPtr)4096, (IntPtr)size);
                    if (rc != 0)
                        Syscall.ThrowLastError(rc, "Could not allocate memory");

                    return ptr;
                }

                var allocate4KbAllignedMemory = Win32MemoryProtectMethods.VirtualAlloc(null, (UIntPtr)size, Win32MemoryProtectMethods.AllocationType.COMMIT,
                    Win32MemoryProtectMethods.MemoryProtection.READWRITE);

                if (allocate4KbAllignedMemory == null)
                    ThrowFailedToAllocate();

                return allocate4KbAllignedMemory;
            }

            public static void Free4KbAlignedMemory(byte* ptr, int size, Sparrow.Utils.NativeMemory.ThreadStats stats)
            {
                Debug.Assert(ptr != null);

                var currentThreadValue = Sparrow.Utils.NativeMemory.ThreadAllocations.Value;
                if (currentThreadValue == stats)
                {
                    currentThreadValue.Allocations -= size;
                    Sparrow.Utils.NativeMemory.FixupReleasesFromOtherThreads(currentThreadValue);
                }
                else
                {
                    Interlocked.Add(ref stats.ReleasesFromOtherThreads, size);
                }

                Interlocked.Add(ref Sparrow.Utils.NativeMemory._totalAllocatedMemory, -size);
                var p = new IntPtr(ptr);
                if (PlatformDetails.RunningOnPosix)
                {
                    Syscall.free(p);
                    return;
                }

                if (Win32MemoryProtectMethods.VirtualFree(ptr, UIntPtr.Zero, Win32MemoryProtectMethods.FreeType.MEM_RELEASE) == false)
                    ThrowFailedToFree();
            }

            private static void ThrowFailedToAllocate()
            {
                throw new Win32Exception("Could not allocate memory");
            }

            private static void ThrowFailedToFree()
            {
                throw new Win32Exception("Failed to free memory");
            }

        }
    }
}
