namespace Nevar.Trees
{
    public class TreeDataInTransaction
    {
        private readonly Tree _tree;
        public Page Root;

        public long BranchPages;
        public long LeafPages;
        public long OverflowPages;
        public int Depth;
        public long PageCount;
        public long EntriesCount;

        public TreeDataInTransaction(Tree tree)
        {
            _tree = tree;
            Root = tree.Root;
            BranchPages = tree.BranchPages;
            LeafPages = tree.LeafPages;
            OverflowPages = tree.OverflowPages;
            Depth = tree.Depth;
            PageCount = tree.PageCount;
            EntriesCount = tree.EntriesCount;
        }

        public void RecordNewPage(Page p, int num)
        {
            PageCount++;
            var flags = p.Flags;
            if (flags.HasFlag(PageFlags.Branch))
            {
                BranchPages++;
            }
            else if (flags.HasFlag(PageFlags.Leaf))
            {
                LeafPages++;
            }
            else if (flags.HasFlag(PageFlags.Overlfow))
            {
                OverflowPages += num;
            }
        }

        public void Flush()
        {
            _tree.BranchPages = BranchPages;
            _tree.LeafPages = LeafPages;
            _tree.OverflowPages = OverflowPages;
            _tree.Depth = Depth;
            _tree.PageCount = PageCount;
            _tree.Root = Root;
            _tree.EntriesCount = EntriesCount;
        }
    }
}