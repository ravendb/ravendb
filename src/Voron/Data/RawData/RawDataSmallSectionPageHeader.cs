using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Data.BTrees;

namespace Voron.Data.RawData
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct RawDataSmallSectionPageHeader
    {
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public int NumberOfEntries;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public RawDataPageFlags RawDataFlags;

        [FieldOffset(14)]
        public ushort LastUsedPage;

        [FieldOffset(16)]
        public int AllocatedSize;

        [FieldOffset(20)]
        public ushort NumberOfPages;

        [FieldOffset(22)]
        public ulong SectionOwnerHash;
    }
}
