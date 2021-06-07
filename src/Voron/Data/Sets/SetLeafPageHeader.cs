using System.Runtime.InteropServices;

namespace Voron.Data.Sets
{
    /*
     * Format of a set leaf page:
     *
     * PageHeader       - 64 bytes
     * 0 - 64 bytes    -  short[16] PositionsOfCompressedEntries; (sorted by value)
     * 
     * actual compressed entries
     *
     * From the top: 0 - 256 values
     * int[] RawValues;  // sorted array
     */
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public struct SetLeafPageHeader
    {
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public ushort CompressedValuesCeiling; 

        [FieldOffset(10)]
        public ushort NumberOfRawValues;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public SetPageFlags SetFlags;
        
        [FieldOffset(14)]
        public byte NumberOfCompressedPositions;

        [FieldOffset(15)]
        public byte Reserved;
        
        [FieldOffset(16)]
        public long Baseline;
    }
}
