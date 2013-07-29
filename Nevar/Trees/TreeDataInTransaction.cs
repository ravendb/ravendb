using Nevar.Impl;

namespace Nevar.Trees
{
    public class TreeDataInTransaction
    {
        private readonly TreeMutableState _state;
        private readonly Tree _tree;
        private Page _root;

        public TreeMutableState State
        {
            get { return _state; }
        }

        public Page Root
        {
            get { return _root; }
            set
            {
                _root = value;
                _state.RootPageNumber = _root.PageNumber;
            }
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
            _tree.SetState(_state);
        }
    }
}