using System.Runtime.InteropServices;

namespace Voron.Data.PostingLists
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public struct PostingListBranchPageHeader
    {
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public int Reserved; 

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public ExtendedPageType SetFlags;
        
        [FieldOffset(14)]
        public ushort NumberOfEntries;

        [FieldOffset(16)]
        public ushort Upper;
    }
}
