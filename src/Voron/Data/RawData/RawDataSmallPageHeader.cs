using System.Diagnostics;
using System.Runtime.InteropServices;

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

        [FieldOffset(22)]
        public ulong SectionOwnerHash;

        [FieldOffset(30)]
        public byte TableType;
    }
}
