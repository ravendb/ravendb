using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Sparrow.Platform.Win32;

namespace Sparrow.Platform
{
    internal unsafe class Win32ElectricFencedMemory
    {
        public static byte* Allocate(int size)
        {
            var remaining = size % 4096;
            var sizeInPages = (size / 4096) + (remaining == 0 ? 0 : 1);

            var allocatedSize = ((sizeInPages + 2) * 4096);
            var virtualAlloc = Win32MemoryProtectMethods.VirtualAlloc(null, (UIntPtr)allocatedSize, Win32MemoryProtectMethods.AllocationType.COMMIT,
                Win32MemoryProtectMethods.MemoryProtection.READWRITE);
            if (virtualAlloc == null)
                throw new Win32Exception(Marshal.GetLastWin32Error(), "Failed to VirtualAlloc (ElectricFence) size=" + size);

            *(int*)virtualAlloc = allocatedSize;

            Win32MemoryProtectMethods.MemoryProtection protect;
            if (Win32MemoryProtectMethods.VirtualProtect(virtualAlloc, (UIntPtr)(4096), Win32MemoryProtectMethods.MemoryProtection.NOACCESS,
                    out protect) == false)
                throw new Win32Exception(Marshal.GetLastWin32Error(), "Failed to VirtualProtect (ElectricFence) at address=" + new IntPtr(virtualAlloc));

            if (Win32MemoryProtectMethods.VirtualProtect(virtualAlloc + (sizeInPages + 1) * 4096, (UIntPtr)(4096), Win32MemoryProtectMethods.MemoryProtection.NOACCESS,
                    out protect) == false)
                throw new Win32Exception(Marshal.GetLastWin32Error(), "Failed to VirtualProtect (ElectricFence) at address=" + new IntPtr(virtualAlloc + (sizeInPages + 1) * 4096));

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
            Win32MemoryProtectMethods.MemoryProtection protect;
            var address = firstWritablePage - 4096;
            // this will access the memory, which will error if this was already freed
            if (Win32MemoryProtectMethods.VirtualProtect(address, (UIntPtr)4096, Win32MemoryProtectMethods.MemoryProtection.READWRITE, out protect) ==
                false)
                throw new Win32Exception(Marshal.GetLastWin32Error(), "Failed to VirtualProtect (ElectricFence) at address=" + new IntPtr(address));
            var dwSize = *(int*)address;

            // decommit, not release, they are not available for reuse again, any 
            // future access will throw
            if (Win32MemoryProtectMethods.VirtualFree(address, (UIntPtr)dwSize, Win32MemoryProtectMethods.FreeType.MEM_DECOMMIT) == false)
                throw new Win32Exception(Marshal.GetLastWin32Error(), "Failed to VirtualFree (ElectricFence) at address=" + new IntPtr(address));
        }
    }
}
