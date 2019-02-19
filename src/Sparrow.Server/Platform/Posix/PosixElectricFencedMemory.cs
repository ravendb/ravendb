using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace Sparrow.Server.Platform.Posix
{
    public unsafe class PosixElectricFencedMemory
    {
        public static long usage =0;
        
        public static byte* Allocate(int size)
        {
            var remaining = size % 4096;
            var sizeInPages = (size / 4096) + (remaining == 0 ? 0 : 1);

            var allocatedSize = ((sizeInPages + 2) * 4096); 


            var virtualAlloc = (byte*)Syscall.mmap64(IntPtr.Zero, (UIntPtr)allocatedSize, MmapProts.PROT_NONE,
                MmapFlags.MAP_PRIVATE | MmapFlags.MAP_ANONYMOUS, -1, 0L);

            if (virtualAlloc == (byte*)-1)
            {
                var err = Marshal.GetLastWin32Error();
                throw new InvalidOperationException("Failed to mmap with size " + allocatedSize + " . Err=" + err);
            }

            var msync = Syscall.msync((IntPtr)virtualAlloc, (UIntPtr)(uint)allocatedSize, MsyncFlags.MS_SYNC | MsyncFlags.MS_INVALIDATE);
            if (msync != 0)
            {
                throw new InvalidOperationException("Failed to free call msync " + (IntPtr)virtualAlloc + ". Err=" + Marshal.GetLastWin32Error());
            }

            virtualAlloc = (byte*)Syscall.mmap64((IntPtr)virtualAlloc, (UIntPtr)allocatedSize, 
                MmapProts.PROT_READ | MmapProts.PROT_WRITE,
                MmapFlags.MAP_FIXED | MmapFlags.MAP_SHARED | MmapFlags.MAP_ANONYMOUS, -1, 0L);

            if (virtualAlloc == (byte*)-1)
            {
                var err = Marshal.GetLastWin32Error();
                throw new InvalidOperationException("Failed to re-mmap with size " + allocatedSize + " . Err=" + err);
            }

             msync = Syscall.msync((IntPtr)virtualAlloc, (UIntPtr)(uint)allocatedSize, MsyncFlags.MS_SYNC | MsyncFlags.MS_INVALIDATE);
            if (msync != 0)
            {
                throw new InvalidOperationException("Failed to free call msync " + (IntPtr)virtualAlloc + ". Err=" + Marshal.GetLastWin32Error());
            }

            *(int*)virtualAlloc = allocatedSize;

            if (Syscall.mprotect((IntPtr)virtualAlloc, new IntPtr(4096), ProtFlag.PROT_NONE) != 0)
            {
                throw new InvalidOperationException("Could not mark the memory as inaccessible because " +
                                               Marshal.GetLastWin32Error());

            }
            
            if (Syscall.mprotect((IntPtr)(virtualAlloc + 4096 + (sizeInPages*4096)), new IntPtr(4096), ProtFlag.PROT_NONE) != 0)
            {
                throw new InvalidOperationException("Could not mark the memory as inaccessible because " +
                                               Marshal.GetLastWin32Error());

            }

            Interlocked.Add(ref usage, allocatedSize);

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

            

            if (Syscall.mprotect((IntPtr)address, new IntPtr(4096), ProtFlag.PROT_READ) != 0)
            {
                throw new InvalidOperationException("Could not mark the memory as readable because " +
                                               Marshal.GetLastWin32Error());

            }
            var dwSize = *(int*)address;


            // var virtualAlloc = (byte*)Syscall.mmap((IntPtr)address, (UIntPtr)dwSize, MmapProts.PROT_NONE,
            //     MmapFlags.MAP_FIXED | MmapFlags.MAP_PRIVATE | MmapFlags.MAP_ANONYMOUS, -1, IntPtr.Zero);


            // if (virtualAlloc == null)
            // {
            //     var err = Marshal.GetLastWin32Error();
            //     throw new InvalidOperationException("Failed to free with " + (IntPtr)address + ". Err=" + err);
            // }
            // var msync = Syscall.msync((IntPtr)virtualAlloc, (UIntPtr)(uint)dwSize, MsyncFlags.MS_SYNC | MsyncFlags.MS_INVALIDATE);
            // if (msync != 0)
            // {
            //     throw new InvalidOperationException("Failed to free call msync " + (IntPtr)address + ". Err=" + Marshal.GetLastWin32Error());
            // }
            Syscall.munmap((IntPtr)address, (UIntPtr)(uint)dwSize);

              Interlocked.Add(ref usage, -dwSize);
        }
    }
}
