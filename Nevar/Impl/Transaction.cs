using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Nevar.Trees;
using System.Linq;

namespace Nevar.Impl
{
	public class Transaction : IDisposable
	{
		public int NextPageNumber;

		private readonly IVirtualPager _pager;
		private readonly StorageEnvironment _env;
		private readonly long _id;
		private readonly List<Page> freePages = new List<Page>();

		private readonly Dictionary<Tree, Cursor> cursors = new Dictionary<Tree, Cursor>();
		private long _oldestTx;

		public Transaction(IVirtualPager pager, StorageEnvironment env, long id)
		{
			_pager = pager;
			_env = env;
			_oldestTx = _env.OldestTransaction;
			_id = id;
			NextPageNumber = env.NextPageNumber;
		}

		public Page GetPage(int n)
		{
			return _pager.Get(n);
		}

		public unsafe Page AllocatePage(int num)
		{
			var page = TryAllocateFromFreeSpace(num);
			if (page != null)
			{
				page.Lower = (ushort)Constants.PageHeaderSize;
				page.Upper = Constants.PageSize;
				
				return page;
			}

		
			page = _pager.Get(NextPageNumber);
			page.PageNumber = NextPageNumber;
			page.Lower = (ushort)Constants.PageHeaderSize;
			page.Upper = Constants.PageSize;

			NextPageNumber += num;
			return page;
		}

		private unsafe Page TryAllocateFromFreeSpace(int num)
		{
			if (_env.FreeSpace == null)
				return null;// TODO: fix me!
			var page = _env.FreeSpace.FindPageFor(this, Slice.BeforeAllKeys, GetCursor(_env.FreeSpace));
			while (true)
			{
				if (page.NumberOfEntries <= 0)
					return null; // there is no free space
				var node = page.GetNode(0);
				var txId = new Slice(node).ToInt64();
				if (txId >= _oldestTx)
					return null; // all the free space is tied up in active transactions

				// now need to find enough pages 

				var pageCount = GetNodeDataSize(node) / sizeof(int);
				
				if (num > pageCount) // not enough free pages, let us try the next transaction
				{
					
				}
			}
		}

		private unsafe int GetNodeDataSize(NodeHeader* node)
		{
			if (node->Flags.HasFlag(NodeFlags.PageRef)) // lots of data, enough to overflow!
			{
				var overflowPage = GetPage(node->PageNumber);
				return overflowPage.OverflowSize;
			}
			return node->DataSize;
		}

		public void Commit()
		{
			_env.NextPageNumber = NextPageNumber;
			
			FlushFreePages();

			foreach (var cursor in cursors.Values)
			{
				cursor.Flush();
			}
		}

		private void FlushFreePages()
		{
			var slice = new Slice(SliceOptions.Key);
			slice.Set(_id);
			while (freePages.Count != 0)
			{
				using (var ms = new MemoryStream())
				using (var binaryWriter = new BinaryWriter(ms))
				{
					foreach (var freePage in freePages.OrderBy(x => x.PageNumber))
					{
						binaryWriter.Write(freePage.PageNumber);
					}

					freePages.Clear();

					var end = ms.Position;
					ms.Position = 0;

					// this may cause additional pages to be freed, so we need need the while loop to track them all
					_env.FreeSpace.Add(this, slice, ms);
					ms.Position = end; // so if we have additional freed pages, they will be added
				}
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
			freePages.Add(page);
		}
	}
}