using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Nevar.Impl.FileHeaders;
using Nevar.Trees;
using System.Linq;

namespace Nevar.Impl
{
	public class Transaction : IDisposable
	{
		public long NextPageNumber;

		private readonly IVirtualPager _pager;
		private readonly StorageEnvironment _env;
		private readonly long _id;
		private readonly HashSet<long> _freePages = new HashSet<long>();

        private readonly Dictionary<Tree, TreeDataInTransaction> _treesInfo = new Dictionary<Tree, TreeDataInTransaction>();
        private readonly Dictionary<long, Page> _dirtyPages = new Dictionary<long, Page>();
		private readonly long _oldestTx;

        public TransactionFlags Flags { get; private set; }

		public Transaction(IVirtualPager pager, StorageEnvironment env, long id, TransactionFlags flags)
		{
			_pager = pager;
			_env = env;
			_oldestTx = _env.OldestTransaction;
			_id = id;
		    Flags = flags;
		    NextPageNumber = env.NextPageNumber;
		}

        public Page ModifyCursor(Tree tree, Cursor c)
        {
            var txInfo = GetTreeInformation(tree);
            return ModifyCursor(txInfo, c);
        }

	    public Page ModifyCursor(TreeDataInTransaction txInfo, Cursor c)
	    {
	        txInfo.Root = ModifyPage(null, txInfo.Root);

	        if (c.Pages.Count == 0)
	            return null;

	        var node = c.Pages.Last;
	        while (node != null)
	        {
                var parent = node.Next != null ? node.Next.Value : null;
	            node.Value = ModifyPage(parent, node.Value);
	            node = node.Previous;
	        }
	        return c.Pages.First.Value;
	    }

	    private unsafe Page ModifyPage(Page parent, Page p)
	    {
	        if (p.Dirty)
	            return p;

            Page page;
	        if (_dirtyPages.TryGetValue(p.PageNumber, out page))
	        {
                page.LastMatch = p.LastMatch;
                page.LastSearchPosition = p.LastSearchPosition;
                UpdateParentPageNumber(parent, page.PageNumber); 
                return page;
	        }

	        var newPage = AllocatePage(1);
	        newPage.Dirty = true;
	        var newPageNum = newPage.PageNumber;
	        NativeMethods.memcpy(newPage.Base, p.Base, Constants.PageSize);
	        newPage.PageNumber = newPageNum;
	        newPage.LastMatch = p.LastMatch;
	        newPage.LastSearchPosition = p.LastSearchPosition;
	        _dirtyPages[p.PageNumber] = newPage;
	        FreePage(p.PageNumber);
            UpdateParentPageNumber(parent, newPage.PageNumber); 
	        return newPage;
	    }

	    private static unsafe void UpdateParentPageNumber(Page parent, long pageNumber)
	    {
	        if (parent == null) 
                return;

            if (parent.Dirty == false)
                throw new InvalidOperationException("The parent page must already been dirtied, but wasn't");
	     
	        var node = parent.GetNode(parent.LastSearchPosition);
	        node->PageNumber = pageNumber;
	    }

	    public Page GetReadOnlyPage(long n)
		{
		    Page page;
		    if (_dirtyPages.TryGetValue(n, out page))
		        return page;
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
	        page.Dirty = true;
	        _dirtyPages[page.PageNumber] = page;
			return page;
		}

		private unsafe Page TryAllocateFromFreeSpace(int num)
		{
			if (_env.FreeSpace == null)
				return null;// this can happen the first time FreeSpace tree is created

			using (var iterator = _env.FreeSpace.Iterate(this))
			{
				if (iterator.Seek(Slice.BeforeAllKeys) == false)
					return null;
				var slice = new Slice(SliceOptions.Key);
				do
				{
					var node = iterator.Current;
					slice.Set(node);

					var txId = slice.ToInt64();

					if (_oldestTx != 0 && txId >= _oldestTx)
						return null;  // all the free space is tied up in active transactions
					var remainingPages = GetNumberOfFreePages(node);

					if (remainingPages < num)
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
					return _pager.Get(list[start]);
				} while (iterator.MoveNext());
				return null;
			}
		}

	    internal unsafe int GetNumberOfFreePages(NodeHeader* node)
	    {
	        return GetNodeDataSize(node)/Constants.PageNumberSize;
	    }

	    private unsafe List<long> ReadRemainingPagesList(int remainingPages, NodeHeader* node)
		{
            var list = new List<long>(remainingPages);
			using (var data = Tree.StreamForNode(this, node))
			using (var reader = new BinaryReader(data))
			{
				for (int i = 0; i < remainingPages; i++)
				{
					list.Add(reader.ReadInt64());
				}
			}
			return list;
		}

		internal unsafe int GetNodeDataSize(NodeHeader* node)
		{
			if (node->Flags.HasFlag(NodeFlags.PageRef)) // lots of data, enough to overflow!
			{
				var overflowPage = GetReadOnlyPage(node->PageNumber);
				return overflowPage.OverflowSize;
			}
			return node->DataSize;
		}

		public void Commit()
		{
		    if (Flags.HasFlag(TransactionFlags.ReadWrite) == false)
		        return; // nothing to do

			FlushFreePages();

			foreach (var cursor in _treesInfo.Values)
			{
				cursor.Flush();
			}

            _env.NextPageNumber = NextPageNumber;
 
            // Because we don't know in what order the OS will flush the pages 
            // we need to do this twice, once for the data, and then once for the metadata
		    _pager.Flush();

		    WriteHeader(_pager.Get(_id & 1)); // this will cycle between the first and second pages

            _pager.Flush(); // and now we flush the metadata as well
		}

	    private unsafe void WriteHeader(Page pg)
	    {
	        var fileHeader = (FileHeader*) pg.Base;
	        fileHeader->TransactionId = _id;
	        fileHeader->LastPageNumber = NextPageNumber - 1;
	        _env.FreeSpace.CopyTo(&fileHeader->FreeSpace);
            _env.Root.CopyTo(&fileHeader->Root);
	    }

	    private void FlushFreePages()
		{
			var slice = new Slice(SliceOptions.Key);
			slice.Set(_id);
			while (_freePages.Count != 0)
			{
				using (var ms = new MemoryStream())
				using (var binaryWriter = new BinaryWriter(ms))
				{
					foreach (var freePage in _freePages.OrderBy(x => x))
					{
						binaryWriter.Write(freePage);
					}

					_freePages.Clear();

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

		public TreeDataInTransaction GetTreeInformation(Tree tree)
		{
            TreeDataInTransaction c;
			if (_treesInfo.TryGetValue(tree, out c))
			{
				return c;
			}
            c = new TreeDataInTransaction(tree);
			_treesInfo.Add(tree, c);
			return c;
		}

		public void FreePage(long pageNumber)
		{
		    var success = _freePages.Add(pageNumber);
		    Debug.Assert(success);
		}

	    public Page GetModifiedPage(Page parentPage, long n)
	    {

	        return ModifyPage(parentPage, GetReadOnlyPage(n));
	    }
	}
}