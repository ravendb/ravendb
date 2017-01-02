using System.Runtime.InteropServices;

namespace Voron.Data.BTrees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public struct TreePageHeader
    {
        public const int SizeOf = PageHeader.SizeOf > 18 ? PageHeader.SizeOf : 18;

        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public int OverflowSize;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public TreePageFlags TreeFlags;

        [FieldOffset(14)]
        public ushort Lower;

        [FieldOffset(16)]
        public ushort Upper;
    }
}