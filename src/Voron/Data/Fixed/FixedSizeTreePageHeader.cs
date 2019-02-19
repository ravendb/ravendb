using System.Runtime.InteropServices;

namespace Voron.Data.Fixed
{

    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public struct FixedSizeTreePageHeader
    {
        public const int SizeOf = PageHeader.SizeOf;

        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public ushort StartPosition;

        [FieldOffset(10)]
        public ushort NumberOfEntries;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public FixedSizeTreePageFlags TreeFlags;

        [FieldOffset(14)]
        public ushort ValueSize;
    }
}
