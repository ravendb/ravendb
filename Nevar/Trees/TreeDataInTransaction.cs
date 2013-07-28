namespace Nevar.Trees
{
    public class TreeDataInTransaction
    {
        private readonly TreeMutableState _state;
        private readonly Tree _tree;

        public Page Root { get { return _state.Root; } }

        public TreeMutableState State
        {
            get { return _state; }
        }


        public TreeDataInTransaction(Tree tree)
        {
            _tree = tree;
            _state = _tree.State.Clone();
        }

        public void RecordNewPage(Page p, int num)
        {
            _state.PageCount++;
            var flags = p.Flags;
            if (flags.HasFlag(PageFlags.Branch))
            {
                _state.BranchPages++;
            }
            else if (flags.HasFlag(PageFlags.Leaf))
            {
                _state.LeafPages++;
            }
            else if (flags.HasFlag(PageFlags.Overlfow))
            {
                _state.OverflowPages += num;
            }
        }

        public void Flush()
        {
            _state.Root.Dirty = false;
            _tree.SetState(_state);
        }
    }
}