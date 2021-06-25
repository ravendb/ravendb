using System.Runtime.InteropServices;

namespace Voron.Data.CompactTrees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public struct CompactPageHeader
    {
        public const int SizeOf = PageHeader.SizeOf;

        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public ushort Lower;

        [FieldOffset(10)]
        public ushort Upper;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public CompactPageFlags PageFlags;

        [FieldOffset(14)]
        public ushort FreeSpace;

        [FieldOffset(16)]
        public long DictionaryId;

        public int NumberOfEntries
        {
            get
            {
                int floor = Lower - PageHeader.SizeOf;
                return floor / sizeof(short);
            }
        }
    }
}
