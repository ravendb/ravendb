using System.Runtime.InteropServices;

namespace Voron.Data.CompactTrees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public unsafe struct CompactPageHeader
    {
        public const int SizeOf = PageHeader.SizeOf;

        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public int Reserved1;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public CompactPageFlags PageFlags;

        [FieldOffset(14)]
        public ushort Lower;

        [FieldOffset(16)]
        public ushort Upper;

        [FieldOffset(18)]
        public ushort FreeSpace;

        public int NumberOfEntries
        {
            get
            {
                int floor = Lower - PageHeader.SizeOf - CompactTree.DictionarySize;
                return floor / sizeof(short);
            }
        }
    }
}
