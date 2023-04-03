using System.Runtime.InteropServices;

namespace Voron.Data.PostingLists
{
    /*
     * Format of a set leaf page:
     *
     * PageHeader       - 64 bytes
     * 0 - 64 bytes    -  short[16] PositionsOfCompressedEntries; (sorted by value)
     * 
     * actual compressed entries
     */
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public unsafe struct PostingListLeafPageHeader
    {
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public ushort NumberOfCompressedRuns;

        [FieldOffset(10)]
        public ushort Ceiling;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public ExtendedPageType PostingListFlags;

        [FieldOffset(24)]
        public int NumberOfEntries;

        public int Floor => PageHeader.SizeOf + (NumberOfCompressedRuns * sizeof(PostingListLeafPage.CompressedHeader));
    }
}
