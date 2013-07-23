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
		private readonly StorageEnvironment _env;

		private readonly Dictionary<Tree, Cursor> cursors = new Dictionary<Tree, Cursor>();

		public Transaction(Pager pager, StorageEnvironment env)
		{
			_pager = pager;
			_env = env;
			NextPageNumber = env.NextPageNumber;
		}

		public Page GetPage(int n)
		{
			return _pager.Get(n);
		}

		public Page AllocatePage(int num)
		{
			var allocatePage = _pager.Allocate(this.NextPageNumber, num);
			NextPageNumber += num;
			DirtyPages.Add(allocatePage);
			return allocatePage;
		}

		public void Commit()
		{
			_env.NextPageNumber = NextPageNumber;
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
			//todo: actually release this
		}
	}
}