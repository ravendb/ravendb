using System;
using Voron.Global;

namespace Voron.Platform
{
    public unsafe class PalDefinitions
    {
        public const int AllocationGranularity = 64 * Constants.Size.Kilobyte;
        public struct SystemInformation
        {
            public Int32 PageSize;
            private Int32 PrefetchOption;

            public bool CanPrefetch => PrefetchOption == 1;
        }

        public struct PrefetchRanges
        {
            public void* VirtualAddress;
            public Int32 NumberOfBytes;
        }
    }
}
