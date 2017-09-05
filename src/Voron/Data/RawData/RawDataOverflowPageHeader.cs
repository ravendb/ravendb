using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Voron.Data.RawData
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public unsafe struct RawDataOverflowPageHeader
    {
        public const int SizeOf = PageHeader.SizeOf;

        static RawDataOverflowPageHeader()
        {
            Debug.Assert(sizeof(RawDataOverflowPageHeader) == SizeOf);
        }

        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public RawDataPageFlags RawDataFlags;

        [FieldOffset(16)]
        public ulong SectionOwnerHash;

        [FieldOffset(24)]
        public byte TableType;
    }
}
