using System;
using System.Text;

namespace Voron.Data.PostingLists
{
    public unsafe struct PostingListCursorState
    {
        public Page Page;
        public int LastMatch;
        public int LastSearchPosition;
        public PostingListLeafPageHeader* LeafHeader => (PostingListLeafPageHeader*)Page.Pointer;
        public PostingListBranchPageHeader* BranchHeader => (PostingListBranchPageHeader*)Page.Pointer;

        public bool IsLeaf => LeafHeader->PostingListFlags == ExtendedPageType.PostingListLeaf;

        public PostingListCursorState(Page page)
        {
            Page = page;
            LastMatch = 0;
            LastSearchPosition = 0;
        }

        public override string ToString()
        {
            if (Page.Pointer == null)
                return "<null state>";

            return $"{nameof(Page)}: {Page.PageNumber} - {nameof(LastMatch)} : {LastMatch}, " +
                   $"{nameof(LastSearchPosition)} : {LastSearchPosition}";
        }
    }
}
