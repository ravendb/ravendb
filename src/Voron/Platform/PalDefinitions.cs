using System;
using System.Diagnostics.CodeAnalysis;
// ReSharper disable BuiltInTypeReferenceStyle

namespace Voron.Platform
{
    public unsafe class PalDefinitions
    {
        public static Int32 True => 1;
        public static Int32 False => 0;

        [SuppressMessage("ReSharper", "UnassignedField.Global")]
        public struct SystemInformation
        {
            public Int32 PageSize;
            public Int32 CanPrefetch;
        }

        [SuppressMessage("ReSharper", "UnusedMember.Global")]
        public struct PrefetchRanges
        {
            public void* VirtualAddress;
            public Int32 NumberOfBytes;
        }
    }
}
