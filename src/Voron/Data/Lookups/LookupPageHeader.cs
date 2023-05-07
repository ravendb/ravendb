using System.Runtime.InteropServices;

namespace Voron.Data.Lookups
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public struct LookupPageHeader
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
        public LookupPageFlags PageFlags;

        [FieldOffset(14)]
        public ushort FreeSpace;

        [FieldOffset(16)]
        public long KeysBase;
        
        [FieldOffset(24)]
        public long ValuesBase;
        
        public int NumberOfEntries
        {
            get
            {
                int floor = Lower - PageHeader.SizeOf;
                return floor / sizeof(short);
            }
        }

        public bool IsBranch => (((byte)PageFlags) & (byte)LookupPageFlags.Branch) != 0;

        public bool IsLeaf => (((byte)PageFlags) & (byte)LookupPageFlags.Leaf) != 0;
    }
}
