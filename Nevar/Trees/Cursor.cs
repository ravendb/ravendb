using System;
using System.Collections.Generic;

namespace Nevar.Trees
{
	public class Cursor
	{
		private readonly Tree _tree;
		public Page Root;

		public long BranchPages;
        public long LeafPages;
        public long OverflowPages;
        public int Depth;
        public long PageCount;

		public LinkedList<Page> Pages = new LinkedList<Page>();

		public Cursor(Tree tree)
		{
			_tree = tree;
			Root = tree.Root;
			BranchPages = tree.BranchPages;
			LeafPages = tree.LeafPages;
			OverflowPages = tree.OverflowPages;
			Depth = tree.Depth;
			PageCount = tree.PageCount;
		}

		public Page ParentPage
		{
			get
			{

				var linkedListNode = Pages.First;
				if (linkedListNode == null)
					throw new InvalidOperationException("No pages in cursor");
				linkedListNode = linkedListNode.Next;
				if (linkedListNode == null)
					throw new InvalidOperationException("No parent page in cursor");
				return linkedListNode.Value;
			}
		}

		public Page CurrentPage
		{
			get
			{
				var linkedListNode = Pages.First;
				if (linkedListNode == null)
					throw new InvalidOperationException("No pages in cursor");
				return linkedListNode.Value;
			}
		}

		public void Push(Page p)
		{
			Pages.AddFirst(p);
		}

		public Page Pop()
		{
			if (Pages.Count == 0)
			{
				throw new InvalidOperationException("No page to pop");
			}
			Page p = Pages.First.Value;
			Pages.RemoveFirst();
			return p;
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
		}
	}
}