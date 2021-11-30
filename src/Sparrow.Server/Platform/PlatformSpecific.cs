using System;
using System.ComponentModel;
using System.Diagnostics;
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
                    // we pass NULL (IntPtr.Zero) as the first parameter (address / start) so the kernel chooses the(page-aligned) address at which to create the mapping

                    var pageAlignedMemory = Syscall.mmap64(IntPtr.Zero, (UIntPtr)size, MmapProts.PROT_READ | MmapProts.PROT_WRITE,
                        MmapFlags.MAP_PRIVATE | MmapFlags.MAP_ANONYMOUS, -1, 0L);

                    if (pageAlignedMemory.ToInt64() == -1)
                    {
                        var err = Marshal.GetLastWin32Error();
                        Syscall.ThrowLastError(err,
                            $"Could not allocate memory (allocation size: {size / Constants.Size.Kilobyte:#,#0} kb)");
                    }

                    return (byte*)pageAlignedMemory;
                }

                var allocate4KbAlignedMemory = Win32MemoryProtectMethods.VirtualAlloc(null, (UIntPtr)size, Win32MemoryProtectMethods.AllocationType.COMMIT,
                    Win32MemoryProtectMethods.MemoryProtection.READWRITE);

                if (allocate4KbAlignedMemory == null)
                    ThrowFailedToAllocate();

                return allocate4KbAlignedMemory;
            }

            public static void Free4KbAlignedMemory(byte* ptr, long size, Sparrow.Utils.NativeMemory.ThreadStats stats)
            {
                Debug.Assert(ptr != null);

                if (stats != null)
                    Sparrow.Utils.NativeMemory.UpdateMemoryStatsForThread(stats, size);

                Interlocked.Add(ref Sparrow.Utils.NativeMemory._totalAllocatedMemory, -size);
                
                if (PlatformDetails.RunningOnPosix)
                {
                    var result = Syscall.munmap((IntPtr)ptr, (UIntPtr)(uint)size);
                    if (result == -1)
                    {
                        var err = Marshal.GetLastWin32Error();
                        Syscall.ThrowLastError(err, "Failed to munmap ");
                    }

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
