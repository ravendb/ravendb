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

        public bool IsLeaf => LeafHeader->SetFlags == SetPageFlags.Leaf;
        
        public override string ToString()
        {
            if (Page.Pointer == null)
                return "<null state>";

            return $"{nameof(Page)}: {Page.PageNumber} - {nameof(LastMatch)} : {LastMatch}, " +
                   $"{nameof(LastSearchPosition)} : {LastSearchPosition}";
        }
    }
}
