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
		private readonly long _oldestTx;

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

		public Page AllocatePage(int num)
		{
			var page = TryAllocateFromFreeSpace(num);
			if (page == null) // allocate from end of file
			{
				page = _pager.Get(NextPageNumber);
				page.PageNumber = NextPageNumber;
				NextPageNumber += num;
			}

			page.Lower = (ushort)Constants.PageHeaderSize;
			page.Upper = Constants.PageSize;

			return page;
		}

		private unsafe Page TryAllocateFromFreeSpace(int num)
		{
			if (_env.FreeSpace == null)
				return null;// TODO: fix me!

			using (var iterator = _env.FreeSpace.Iterage(this))
			{
				if (iterator.Seek(Slice.BeforeAllKeys) == false)
					return null;
				var slice = new Slice(SliceOptions.Key);
				do
				{
					var node = iterator.Current;
					slice.Set(node);

					var txId = slice.ToInt64();

					if (txId >= _oldestTx)
						return null;  // all the free space is tied up in active transactions
					var remainingPages = GetNodeDataSize(node) / sizeof(int);

					if (remainingPages > num)
						continue; // this transaction doesn't have enough pages, let us try the next one...

					// this transaction does have enough pages, now we need to see if we can find enough sequential pages
					var list = ReadRemainingPagesList(remainingPages, node);
					var start = 0;
					var len = 1;
					for (int i = 1; i < list.Count && len < num; i++)
					{
						if (list[i - 1] + 1 != list[i]) // hole found, try from current post
						{
							start = i;
							len = 1;
							continue;
						}
						len++;
					}

					if (len != num)
						continue; // couldn't find enough consecutive entries, try next tx...

					if (remainingPages - len == 0) // we completely emptied this transaction
					{
						_env.FreeSpace.Delete(this, slice);
					}
					else // update with the taken pages
					{
						list.RemoveRange(start, len);
						using (var ms = new MemoryStream())
						using(var writer = new BinaryWriter(ms))
						{
							foreach (var i in list)
							{
								writer.Write(i);
							}
							ms.Position = 0;
							_env.FreeSpace.Add(this, slice, ms);
						}
					}
					return GetPage(list[start]);
				} while (iterator.MoveNext());
				return null;
			}
		}

		private unsafe List<int> ReadRemainingPagesList(int remainingPages, NodeHeader* node)
		{
			var list = new List<int>(remainingPages);
			using (var data = Tree.StreamForNode(this, node))
			using (var reader = new BinaryReader(data))
			{
				for (int i = 0; i < remainingPages; i++)
				{
					list.Add(reader.ReadInt32());
				}
			}
			return list;
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
			_env.TransactionCompleted(_id);
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