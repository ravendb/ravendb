using System;
using System.Text;

namespace Voron.Data.Sets
{
    public unsafe struct SetCursorState
    {
        public Page Page;
        public int LastMatch;
        public int LastSearchPosition;
        public SetLeafPageHeader* LeafHeader => (SetLeafPageHeader*)Page.Pointer;
        public SetBranchPageHeader* BranchHeader => (SetBranchPageHeader*)Page.Pointer;

        public bool IsLeaf => LeafHeader->SetFlags == ExtendedPageType.SetLeaf;

        public SetCursorState(Page page)
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
