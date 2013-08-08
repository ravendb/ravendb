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

		private TreeDataInTransaction _rootTreeData;
		private TreeDataInTransaction _fresSpaceTreeData;
		private readonly Dictionary<Tree, TreeDataInTransaction> _treesInfo = new Dictionary<Tree, TreeDataInTransaction>();
		private readonly Dictionary<long, long> _dirtyPages = new Dictionary<long, long>();

		private readonly HashSet<long> _freedPages = new HashSet<long>();
		private readonly HashSet<PagerState> _pagerStates = new HashSet<PagerState>();
		private FreeSpaceCollector _freeSpaceCollector;

		public TransactionFlags Flags { get; private set; }

		public StorageEnvironment Environment
		{
			get { return _env; }
		}

		public IVirtualPager Pager
		{
			get { return _pager; }
		}

		public long Id
		{
			get { return _id; }
		}

		public Transaction(IVirtualPager pager, StorageEnvironment env, long id, TransactionFlags flags)
		{
			_pager = pager;
			_env = env;
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
		    Debug.Assert(c.Pages.Count > 0); // cannot modify an empty cursor
			
            var node = c.Pages.Last;
			while (node != null)
			{
				var parent = node.Next != null ? node.Next.Value : null;
				node.Value = ModifyPage(parent, node.Value.PageNumber, c);
				node = node.Previous;
			}

		    txInfo.RootPageNumber = c.Pages.Last.Value.PageNumber;

			return c.Pages.First.Value;
		}

		public unsafe Page ModifyPage(Page parent, long p, Cursor c)
		{
			long dirtyPageNum;
			Page page;
			if (_dirtyPages.TryGetValue(p, out dirtyPageNum))
			{
				page = c.GetPage(dirtyPageNum) ?? _pager.Get(this, dirtyPageNum);
				page.Dirty = true;
				UpdateParentPageNumber(parent, page.PageNumber);
				return page;
			}
			var newPage = AllocatePage(1);
			newPage.Dirty = true;
			var newPageNum = newPage.PageNumber;
			page = c.GetPage(p) ?? _pager.Get(this, p);
			NativeMethods.memcpy(newPage.Base, page.Base, _pager.PageSize);
			newPage.LastSearchPosition = page.LastSearchPosition;
			newPage.LastMatch = page.LastMatch;
			newPage.PageNumber = newPageNum;
			FreePage(p);
			_dirtyPages[p] = newPage.PageNumber;
			UpdateParentPageNumber(parent, newPage.PageNumber);
			return newPage;
		}

		private static unsafe void UpdateParentPageNumber(Page parent, long pageNumber)
		{
			if (parent == null)
				return;

			if (parent.Dirty == false)
				throw new InvalidOperationException("The parent page must already been dirtied, but wasn't");

			var node = parent.GetNode(parent.LastSearchPositionOrLastEntry);
			node->PageNumber = pageNumber;
		}

		public Page GetReadOnlyPage(long n)
		{
			long dirtyPage;
			if (_dirtyPages.TryGetValue(n, out dirtyPage))
				n = dirtyPage;
			return _pager.Get(this, n);
		}

		public Page AllocatePage(int num)
		{
			Page page = null;
			if (_freeSpaceCollector != null)
				page = _freeSpaceCollector.TryAllocateFromFreeSpace(this, num);
			if (page == null) // allocate from end of file
			{
				if (num > 1)
					_pager.EnsureContinious(this, NextPageNumber, num);
				page = _pager.Get(this, NextPageNumber);
				page.PageNumber = NextPageNumber;
				NextPageNumber += num;
			}
			page.Lower = (ushort)Constants.PageHeaderSize;
			page.Upper = (ushort)_pager.PageSize;
			page.Dirty = true;
			_dirtyPages[page.PageNumber] = page.PageNumber;
			return page;
		}


		internal unsafe int GetNumberOfFreePages(NodeHeader* node)
		{
			return GetNodeDataSize(node) / Constants.PageNumberSize;
		}

		internal unsafe int GetNodeDataSize(NodeHeader* node)
		{
			if (node->Flags==(NodeFlags.PageRef)) // lots of data, enough to overflow!
			{
				var overflowPage = GetReadOnlyPage(node->PageNumber);
				return overflowPage.OverflowSize;
			}
			return node->DataSize;
		}

		public unsafe void Commit()
		{
			if (Flags!=(TransactionFlags.ReadWrite))
				return; // nothing to do

			foreach (var kvp in _treesInfo)
			{
				var cursor = kvp.Value;
				var tree = kvp.Key;

				cursor.Flush();
				if (string.IsNullOrEmpty(kvp.Key.Name))
					continue;

				var treePtr = (TreeRootHeader*)_env.Root.DirectAdd(this, tree.Name, sizeof(TreeRootHeader));
				tree.State.CopyTo(treePtr);
			}

			byte iterationCount = 1;
			FlushFreePages(ref iterationCount);   // this is the the free space that is available when all concurrent transactions are done

			// this is free space that is available right now, HAS to be after flushing free space, since old space might be used
			// to write the new free pages
			if (_freeSpaceCollector != null)
			{
				var changed = _freeSpaceCollector.SaveOldFreeSpace(this);
				// saving old free space might have create more free space
				// at this point we would won't use any free space and just flush any additional
				// free pages once and for all
				_freeSpaceCollector = null;
				if (changed)
				{
					FlushFreePages(ref iterationCount);
				}
			}

			if (_rootTreeData != null)
				_rootTreeData.Flush();

			if (_fresSpaceTreeData != null)
				_fresSpaceTreeData.Flush();

			_env.NextPageNumber = NextPageNumber;

			// Because we don't know in what order the OS will flush the pages 
			// we need to do this twice, once for the data, and then once for the metadata

			var sortedPagesToFlush = _dirtyPages.Select(x => x.Value).Distinct().ToList();
			sortedPagesToFlush.Sort();
			_pager.Flush(sortedPagesToFlush);

			if (_freeSpaceCollector != null)
				_freeSpaceCollector.LastTransactionPageUsage(sortedPagesToFlush.Count);

			WriteHeader(_pager.Get(this, _id & 1)); // this will cycle between the first and second pages

			_pager.Flush(_id & 1); // and now we flush the metadata as well

			_pager.Sync();
		}

		private unsafe void WriteHeader(Page pg)
		{
			var fileHeader = (FileHeader*)pg.Base;
			fileHeader->TransactionId = _id;
			fileHeader->LastPageNumber = NextPageNumber - 1;
			_env.FreeSpace.State.CopyTo(&fileHeader->FreeSpace);
			_env.Root.State.CopyTo(&fileHeader->Root);
		}

		private void FlushFreePages(ref byte iterationCounter)
		{
			var slice = new Slice(SliceOptions.Key);
			// transaction ids in free pages are 56 bits (64 - 8)
			// the right most bits are reserved for iteration counters
			while (_freedPages.Count != 0)
			{
				slice.Set(_id << 8 | iterationCounter);
				iterationCounter++;
				using (var ms = new MemoryStream())
				using (var binaryWriter = new BinaryWriter(ms))
				{
					foreach (var freePage in _freedPages.OrderBy(x => x))
					{
						binaryWriter.Write(freePage);
					}
					_freedPages.Clear();

					ms.Position = 0;

					// this may cause additional pages to be freed, so we need need the while loop to track them all
					_env.FreeSpace.Add(this, slice, ms);
					ms.Position = 0; // so if we have additional freed pages, they will be added
				}
			}
		}

		public void Dispose()
		{
			_env.TransactionCompleted(_id);
			foreach (var pagerState in _pagerStates)
			{
				pagerState.Release();
			}
		}

		public TreeDataInTransaction GetTreeInformation(Tree tree)
		{
			if (tree == _env.Root)
			{
				return _rootTreeData ?? (_rootTreeData = new TreeDataInTransaction(_env.Root)
					{
						RootPageNumber = _env.Root.State.RootPageNumber
					});
			}
			if (tree == _env.FreeSpace)
			{
				return _fresSpaceTreeData ?? (_fresSpaceTreeData = new TreeDataInTransaction(_env.FreeSpace)
					{
						RootPageNumber = _env.FreeSpace.State.RootPageNumber
					});
			}

			TreeDataInTransaction c;
			if (_treesInfo.TryGetValue(tree, out c))
			{
				return c;
			}
			c = new TreeDataInTransaction(tree)
				{
					RootPageNumber = tree.State.RootPageNumber
				};
			_treesInfo.Add(tree, c);
			return c;
		}

		public void FreePage(long pageNumber)
		{
			_dirtyPages.Remove(pageNumber);
#if DEBUG
			Debug.Assert(pageNumber >= 2 && pageNumber <= _pager.NumberOfAllocatedPages);
			var success = _freedPages.Add(pageNumber);
			Debug.Assert(success);
#else
            _freedPages.Add(pageNumber);
#endif
		}


		internal void UpdateRoots(Tree root, Tree freeSpace)
		{
			if (_treesInfo.TryGetValue(root, out _rootTreeData))
			{
				_treesInfo.Remove(root);
			}
			else
			{
				_rootTreeData = new TreeDataInTransaction(root);
			}
			if (_treesInfo.TryGetValue(freeSpace, out _fresSpaceTreeData))
			{
				_treesInfo.Remove(freeSpace);
			}
			else
			{
				_fresSpaceTreeData = new TreeDataInTransaction(freeSpace);
			}
		}

		public void AddPagerState(PagerState state)
		{
			_pagerStates.Add(state);
		}


		public void SetFreeSpaceCollector(FreeSpaceCollector freeSpaceCollector)
		{
			_freeSpaceCollector = freeSpaceCollector;
		}

	    public Cursor NewCursor(Tree tree)
	    {
	        return new Cursor();
	    }
	}
}