using System;
using System.Runtime.InteropServices;

namespace Voron.Data.PostingLists
{
    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public struct PostingListLeafPageHeader
    {
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public int NumberOfEntries;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public ExtendedPageType PostingListFlags;

        [FieldOffset(14)]
        public ushort SizeUsed;

        public int CollapsedLevels
        {
            get => PageCollapsedLevels.Get(PostingListFlags);
            set => PostingListFlags = PageCollapsedLevels.Set(PostingListFlags, value);
        }

        public ExtendedPageType PageType => PageCollapsedLevels.PageType(PostingListFlags);
    }
}
