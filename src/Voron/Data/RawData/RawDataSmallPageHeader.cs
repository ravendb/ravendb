using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Data.BTrees;

namespace Voron.Data.RawData
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public unsafe struct RawDataSmallPageHeader
    {
        public const int SizeOf = PageHeader.SizeOf;

        static RawDataSmallPageHeader()
        {
            Debug.Assert(sizeof(RawDataSmallPageHeader) == SizeOf);
        }

        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public ushort NumberOfEntries;

        [FieldOffset(10)]
        public ushort NextAllocation;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public RawDataPageFlags RawDataFlags;

        [FieldOffset(14)]
        public ushort PageNumberInSection;
    }
}
