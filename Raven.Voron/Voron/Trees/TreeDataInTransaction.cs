using System;

namespace Voron.Trees
{
//    public class TreeDataInTransaction
//    {
//        private readonly TreeMutableState _state;
//	    private readonly Tree _tree;
//
//	    public TreeMutableState State
//        {
//            get { return _state; }
//        }
//
//        public long RootPageNumber
//        {
//            get { return _state.RootPageNumber; }
//            set { _state.RootPageNumber = value; }
//        }
//
//        public Tree Tree
//        {
//            get { return _tree; }
//        }
//
//		public TreeDataInTransaction(Tree tree)
//		{
//			_tree = tree;
//            _state = tree.State.CloneWithMutate();
//        }
//
//        public void RecordNewPage(Page p, int num)
//        {
//            _state.PageCount++;
//            var flags = p.Flags;
//            if (flags == (PageFlags.Branch))
//            {
//                _state.BranchPages++;
//            }
//            else if (flags == (PageFlags.Leaf))
//            {
//                _state.LeafPages++;
//            }
//            else if (flags == (PageFlags.Overflow))
//            {
//                _state.OverflowPages += num;
//            }
//        }
//
//        public override string ToString()
//        {
//            return _tree.Name + " " + _state.EntriesCount;
//        }
//    }
}