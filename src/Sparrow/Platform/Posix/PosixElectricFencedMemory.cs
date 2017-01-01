using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Sparrow.Platform.Posix;
using Voron.Platform.Posix;

namespace Sparrow.Platform
{
    internal unsafe class PosixElectricFencedMemory
    {
        public static byte* Allocate(int size)
        {
            var remaining = size % 4096;
            var sizeInPages = (size / 4096) + (remaining == 0 ? 0 : 1);

            var allocatedSize = ((sizeInPages + 1) * 4096); // linux already put a guard page after each mmap


            var virtualAlloc = (byte*)Syscall.mmap(IntPtr.Zero, (UIntPtr)allocatedSize, MmapProts.PROT_READ | MmapProts.PROT_WRITE,
                MmapFlags.MAP_FIXED | MmapFlags.MAP_SHARED, -1, IntPtr.Zero);

            if (virtualAlloc == null)
            {
                var err = Marshal.GetLastWin32Error();
                throw new OutOfMemoryException("Failed to mmap with size " + allocatedSize + " . Err=" + err);
            }



            *(int*)virtualAlloc = allocatedSize;

            if (Syscall.mprotect((IntPtr)virtualAlloc, 4096, ProtFlag.PROT_NONE) != 0)
            {
                throw new OutOfMemoryException("Could not mark the memory as inaccessible because " +
                                               Marshal.GetLastWin32Error());

            }

            var firstWritablePage = virtualAlloc + 4096;

            Memory.Set(firstWritablePage, 0xED, 4096 * sizeInPages); // don't assume zero'ed mem
            if (remaining == 0)
                return firstWritablePage;
            // give the memory out so its end would be at the 2nd guard page
            return firstWritablePage + (4096 - remaining);

        }

        public static void Free(byte* p)
        {
            var remaining = (int)((long)p % 4096);
            var firstWritablePage = p - remaining;
            for (int i = 0; i < remaining; i++)
            {
                if (firstWritablePage[i] != 0xED)
                    throw new InvalidOperationException("Invalid memory usage, you killed Ed!");
            }
            var address = firstWritablePage - 4096;

            if (Syscall.mprotect((IntPtr)address, 4096, ProtFlag.PROT_READ) != 0)
            {
                throw new OutOfMemoryException("Could not mark the memory as readable because " +
                                               Marshal.GetLastWin32Error());

            }
            var dwSize = *(int*)address;


            var virtualAlloc = (byte*)Syscall.mmap((IntPtr)address, (UIntPtr)dwSize, MmapProts.PROT_NONE,
                MmapFlags.MAP_FIXED | MmapFlags.MAP_PRIVATE | MmapFlags.MAP_ANONYMOUS, -1, IntPtr.Zero);


            if (virtualAlloc == null)
            {
                var err = Marshal.GetLastWin32Error();
                throw new OutOfMemoryException("Failed to free with " + (IntPtr)address + ". Err=" + err);
            }
            var msync = Syscall.msync((IntPtr)virtualAlloc, (UIntPtr)(uint)dwSize, MsyncFlags.MS_SYNC | MsyncFlags.MS_INVALIDATE);
            if (msync != 0)
            {
                throw new OutOfMemoryException("Failed to free call msync " + (IntPtr)address + ". Err=" + Marshal.GetLastWin32Error());
            }

        }
    }
}
