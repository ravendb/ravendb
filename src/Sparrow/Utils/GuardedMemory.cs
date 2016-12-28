using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security;

namespace Sparrow.Utils
{
    public static unsafe class GuardedMemory
    {
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern byte* VirtualAlloc(byte* lpAddress, UIntPtr dwSize,
           AllocationType flAllocationType, MemoryProtection flProtect);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool VirtualProtect(byte* lpAddress, UIntPtr dwSize,
            MemoryProtection flNewProtect, out MemoryProtection lpflOldProtect);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool VirtualFree(byte* lpAddress, UIntPtr dwSize,
            FreeType dwFreeType);

        [DllImport("msvcrt.dll", EntryPoint = "memset", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
        [SecurityCritical]
        private static extern IntPtr memset(byte* dest, int c, long count);


        [Flags]
        private enum AllocationType : uint
        {
            COMMIT = 0x1000,
        }

        [Flags]
        private enum MemoryProtection : uint
        {
            NOACCESS = 0x01,
            READWRITE = 0x04,
        }

        [Flags]
        private enum FreeType : uint
        {
            MEM_DECOMMIT = 0x4000,
            MEM_RELEASE = 0x8000
        }


        public static byte* Allocate(int size)
        {
            var remaining = size % 4096;
            var sizeInPages = (size / 4096) + (remaining == 0 ? 0 : 1);

            var allocatedSize = ((sizeInPages + 2) * 4096);
            var virtualAlloc = VirtualAlloc(null, (UIntPtr)allocatedSize, AllocationType.COMMIT,
                MemoryProtection.READWRITE);
            if (virtualAlloc == null)
                throw new Win32Exception();

            *(int*)virtualAlloc = allocatedSize;

            MemoryProtection protect;
            if (VirtualProtect(virtualAlloc, (UIntPtr)(4096), MemoryProtection.NOACCESS,
                    out protect) == false)
                throw new Win32Exception();

            if (VirtualProtect(virtualAlloc + (sizeInPages + 1) * 4096, (UIntPtr)(4096), MemoryProtection.NOACCESS,
                    out protect) == false)
                throw new Win32Exception();

            var firstWritablePage = virtualAlloc + 4096;

            memset(firstWritablePage, 0xED, 4096 - remaining);

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
            MemoryProtection protect;
            var address = firstWritablePage - 4096;
            if (VirtualProtect(address, (UIntPtr)4096, MemoryProtection.READWRITE, out protect) ==
                false)
                throw new Win32Exception();
            var dwSize = *(int*)address;

            // decommit, not release, they are not available for reuse again, any 
            // future access will throw
            if (VirtualFree(address, (UIntPtr)dwSize, FreeType.MEM_DECOMMIT) == false)
                throw new Win32Exception();
        }
    }

}