using System;
using System.Collections.Generic;
using Nevar.Trees;

namespace Nevar.Impl
{
	public class Transaction : IDisposable
	{
		public int NextPageNumber;
		
		private readonly IVirtualPager _pager;
		private readonly StorageEnvironment _env;

		private readonly Dictionary<Tree, Cursor> cursors = new Dictionary<Tree, Cursor>();

		public Transaction(IVirtualPager pager, StorageEnvironment env)
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
			var page = _pager.Get(NextPageNumber);
			page.PageNumber = NextPageNumber;
			page.Lower = (ushort)Constants.PageHeaderSize;
			page.Upper = Constants.PageSize;
			NextPageNumber += num;
			return page;
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