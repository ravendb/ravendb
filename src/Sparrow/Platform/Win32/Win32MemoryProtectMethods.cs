using System;
using System.Runtime.InteropServices;

namespace Sparrow.Platform.Win32
{
    public static unsafe class Win32MemoryProtectMethods
    {
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern byte* VirtualAlloc(byte* lpAddress, UIntPtr dwSize,
           AllocationType flAllocationType, MemoryProtection flProtect);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool VirtualProtect(byte* lpAddress, UIntPtr dwSize,
            MemoryProtection flNewProtect, out MemoryProtection lpflOldProtect);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool VirtualFree(byte* lpAddress, UIntPtr dwSize,
            FreeType dwFreeType);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern int VirtualQuery(
            byte* lpAddress,
            MEMORY_BASIC_INFORMATION* lpBuffer,
            UIntPtr dwLength
        );

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool DiscardVirtualMemory(void* lpAddress, UIntPtr size);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern int VirtualQueryEx(
            IntPtr hProcess,
            byte* lpAddress,
            MEMORY_BASIC_INFORMATION* lpBuffer,
            UIntPtr dwLength
        );

        [DllImport("psapi.dll", SetLastError = true)]
        public static extern int GetMappedFileName(
            IntPtr hProcess,
            void* lpv,
            byte* lpFilename,
            uint nSize
        );

        [StructLayout(LayoutKind.Sequential)]
        public struct MEMORY_BASIC_INFORMATION
        {
            public UIntPtr BaseAddress;
            public UIntPtr AllocationBase;
            public uint AllocationProtect;
            public IntPtr RegionSize;
            public uint State;
            public uint Protect;
            public uint Type;
        }

        [Flags]
        public enum FreeType : uint
        {
            MEM_DECOMMIT = 0x4000,
            MEM_RELEASE = 0x8000
        }

        [Flags]
        public enum AllocationType : uint
        {
            COMMIT = 0x1000,
            RESERVE = 0x2000,
            RESET = 0x80000,
            LARGE_PAGES = 0x20000000,
            PHYSICAL = 0x400000,
            TOP_DOWN = 0x100000,
            WRITE_WATCH = 0x200000
        }

        [Flags]
        public enum MemoryProtection : uint
        {
            EXECUTE = 0x10,
            EXECUTE_READ = 0x20,
            EXECUTE_READWRITE = 0x40,
            EXECUTE_WRITECOPY = 0x80,
            NOACCESS = 0x01,
            READONLY = 0x02,
            READWRITE = 0x04,
            WRITECOPY = 0x08,
            GUARD_Modifierflag = 0x100,
            NOCACHE_Modifierflag = 0x200,
            WRITECOMBINE_Modifierflag = 0x400
        }
    }
}
