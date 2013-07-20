using System;
using System.Collections.Generic;
using Nevar.Trees;

namespace Nevar.Impl
{
	public class Transaction : IDisposable
	{
		public int NextPageNumber;
		public List<Page> DirtyPages = new List<Page>();

		private readonly Pager _pager;

		private readonly Dictionary<Tree, Cursor> cursors = new Dictionary<Tree, Cursor>();

		public Transaction(Pager pager)
		{
			_pager = pager;
			NextPageNumber = pager.NextPageNumber;
		}

		public Page GetPage(int n)
		{
			return _pager.Get(n);
		}

		public Page AllocatePage(int num)
		{
			return _pager.Allocate(this, num);
		}

		public void Commit()
		{
			_pager.NextPageNumber = NextPageNumber;
			foreach (var cursor in cursors)
			{
				cursor.Value.Flush();
			}
		}

		public void Dispose()
		{
			
		}

		public Cursor GetCursor(Tree tree)
		{
			Cursor c;
			if (cursors.TryGetValue(tree, out c))
			{
				c.Pages.Clear(); // this reset the mutable cursor state
				return c;
			}
			c = new Cursor(tree);
			cursors.Add(tree, c);
			return c;
		}

		public void FreePage(Page page)
		{
			DirtyPages.Remove(page);
			//todo: actually release this
		}
	}
}