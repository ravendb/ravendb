using System;
using System.Runtime.InteropServices;
using Sparrow.Global;

namespace Sparrow.Server.Platform
{
    public unsafe class PalDefinitions
    {
        public const int AllocationGranularity = 64 * Constants.Size.Kilobyte;

        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct SystemInformation
        {
            public Int32 PageSize;
            private Int32 PrefetchOption;

            public bool CanPrefetch => PrefetchOption == 1;
        }

        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct PrefetchRanges
        {
            public void* VirtualAddress;
            public IntPtr NumberOfBytes;
        }
    }
}
