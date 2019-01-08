using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace Voron.Platform
{
    public unsafe class PalDefinitions
    {
        public struct SystemInformation
        {
            public Int32 PageSize;
            [MarshalAs(UnmanagedType.U1)] public bool CanPrefetch;
        }

        public struct PrefetchRanges
        {
            public void* VirtualAddress;
            public Int32 NumberOfBytes;
        }
    }
}
