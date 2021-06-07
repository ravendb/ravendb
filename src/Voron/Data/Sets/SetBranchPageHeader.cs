using System.Runtime.InteropServices;

namespace Voron.Data.Sets
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public struct SetBranchPageHeader
    {
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public int Reserved; 

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public SetPageFlags SetFlags;
        
        [FieldOffset(14)]
        public ushort NumberOfEntries;

        [FieldOffset(16)]
        public ushort Upper;
    }
}
