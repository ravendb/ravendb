using Nevar.Impl.FileHeaders;

namespace Nevar.Trees
{
    public unsafe class TreeMutableState
    {
        public long BranchPages;
        public long LeafPages;
        public long OverflowPages;
        public int Depth;
        public long PageCount;
        public long EntriesCount;

        public long RootPageNumber;

        public void CopyTo(TreeRootHeader* header)
        {
            header->BranchPages = BranchPages;
            header->Depth = Depth;
            header->Flags = TreeFlags.None;
            header->LeafPages = LeafPages;
            header->OverflowPages = OverflowPages;
            header->PageCount = PageCount;
            header->EntriesCount = EntriesCount;
            header->RootPageNumber = RootPageNumber;
        }

        public TreeMutableState Clone()
        {
            return new TreeMutableState
                {
                    BranchPages = BranchPages,
                    Depth = Depth,
                    EntriesCount = EntriesCount,
                    LeafPages = LeafPages,
                    OverflowPages = OverflowPages,
                    PageCount = PageCount,
                    RootPageNumber = RootPageNumber
                };
        }
    }
}