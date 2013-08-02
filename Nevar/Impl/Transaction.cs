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

        private bool _alreadyLookingForFreeSpace;
        private TreeDataInTransaction _rootTreeData;
        private TreeDataInTransaction _fresSpaceTreeData;
        private readonly Dictionary<Tree, TreeDataInTransaction> _treesInfo = new Dictionary<Tree, TreeDataInTransaction>();
        private readonly Dictionary<long, Page> _dirtyPages = new Dictionary<long, Page>();
        private readonly long _oldestTx;

        private HashSet<long> _freePages = new HashSet<long>();
	    private readonly HashSet<PagerState> _pagerStates = new HashSet<PagerState>();
        private List<long> _freeSpace;

        public TransactionFlags Flags { get; private set; }

        public StorageEnvironment Environment
        {
            get { return _env; }
        }

        public IVirtualPager Pager
        {
            get { return _pager; }
        }

        public Transaction(IVirtualPager pager, StorageEnvironment env, long id, TransactionFlags flags)
        {
            _pager = pager;
            _env = env;
            _oldestTx = _env.OldestTransaction;
            _id = id;
            Flags = flags;
            NextPageNumber = env.NextPageNumber;
        }

		/// <summary>
		/// This method will find all the currently free space in the database and make it easily available 
		/// for the transaction. This has to be called _after_ the transaction has already been setup.
		/// </summary>
		public void GatherFreeSpace()
		{
			if (Flags.HasFlag(TransactionFlags.ReadWrite) == false)
				throw new InvalidOperationException("Cannot gather free space in a read only transaction");

			_freeSpace = MergeAllFreeSpaceAvailable();
		}

        public Page ModifyCursor(Tree tree, Cursor c)
        {
            var txInfo = GetTreeInformation(tree);
            return ModifyCursor(txInfo, c);
        }

        public Page ModifyCursor(TreeDataInTransaction txInfo, Cursor c)
        {
            if (txInfo.Root.Dirty == false)
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
            NativeMethods.memcpy(newPage.Base, p.Base, _pager.PageSize);
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

            var node = parent.GetNode(parent.LastSearchPositionOrLastEntry);
            node->PageNumber = pageNumber;
        }

        public Page GetReadOnlyPage(long n)
        {
            Page page;
            if (_dirtyPages.TryGetValue(n, out page))
                return page;
            return _pager.Get(this, n);
        }

        public Page AllocatePage(int num)
        {
            var page = TryAllocateFromFreeSpace(num);
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
            _dirtyPages[page.PageNumber] = page;
            return page;
        }

        private Page TryAllocateFromFreeSpace(int num)
        {
            if (_env.FreeSpace == null)
                return null;// this can happen the first time FreeSpace tree is created

            if (_alreadyLookingForFreeSpace)
                return null;// can't recursively find free space

            _alreadyLookingForFreeSpace = true;
            try
            {

                if (_freeSpace == null || _freeSpace.Count == 0)
                    return null;
                var start = 0;
                var len = 1;
                for (int i = 1; i < _freeSpace.Count && len < num; i++)
                {
                    if (_freeSpace[i - 1] + 1 != _freeSpace[i]) // hole found, try from current page
                    {
                        start = i;
                        len = 1;
                        continue;
                    }
                    len++;
                }

                if (len != num)
                    return null;

                var page = _freeSpace[start];
                _freeSpace.RemoveRange(start, len);
                return _pager.Get(this, page);
            }
            finally
            {
                _alreadyLookingForFreeSpace = false;
            }
        }

        private void SaveOldFreeSpace()
        {
			if (_freeSpace == null || _freeSpace.Count > 0)
                return;
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                foreach (var i in _freeSpace)
                {
                    writer.Write(i);
                }
                ms.Position = 0;
                var slice = new Slice(SliceOptions.Key);
                slice.Set(_oldestTx - 1); // make this space immediately available
                _env.FreeSpace.Add(this, slice, ms);
            }
        }

        private unsafe List<long> MergeAllFreeSpaceAvailable()
        {
            var results = new List<long>();
            if(_env.FreeSpace == null)
                return results;

            var toDelete = new List<Slice>();
            using (var iterator = _env.FreeSpace.Iterate(this))
            {
                if (iterator.Seek(Slice.BeforeAllKeys) == false)
                    return results;

                do
                {
                    var node = iterator.Current;
                    var slice = new Slice(node);

                    var txId = slice.ToInt64();

                    if (_oldestTx != 0 && txId >= _oldestTx)
                        break;  // all the free space after this is tied up in active transactions

                    toDelete.Add(slice);
                    var remainingPages = GetNumberOfFreePages(node);

                    using (var data = Tree.StreamForNode(this, node))
                    using (var reader = new BinaryReader(data))
                    {
                        for (int i = 0; i < remainingPages; i++)
                        {
                            results.Add(reader.ReadInt64());
                        }
                    }

                } while (iterator.MoveNext());
            }

            foreach (var slice in toDelete)
            {
                _env.FreeSpace.Delete(this , slice);
            }
            results.Sort();
            return results;
        }


        internal unsafe int GetNumberOfFreePages(NodeHeader* node)
        {
            return GetNodeDataSize(node) / Constants.PageNumberSize;
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

        public unsafe void Commit()
        {
            if (Flags.HasFlag(TransactionFlags.ReadWrite) == false)
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

            SaveOldFreeSpace(); // this is free space that is available right now

            FlushFreePages();   // this is the the free space that is available when all concurrent transactions are done

            if (_rootTreeData != null)
                _rootTreeData.Flush();

            if (_fresSpaceTreeData != null)
                _fresSpaceTreeData.Flush();

            _env.NextPageNumber = NextPageNumber;

            // Because we don't know in what order the OS will flush the pages 
            // we need to do this twice, once for the data, and then once for the metadata
            _pager.Flush();

            WriteHeader(_pager.Get(this, _id & 1)); // this will cycle between the first and second pages

            _pager.Flush(); // and now we flush the metadata as well
        }

        private unsafe void WriteHeader(Page pg)
        {
            var fileHeader = (FileHeader*)pg.Base;
            fileHeader->TransactionId = _id;
            fileHeader->LastPageNumber = NextPageNumber - 1;
            _env.FreeSpace.State.CopyTo(&fileHeader->FreeSpace);
            _env.Root.State.CopyTo(&fileHeader->Root);
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
                    var old = _freePages;
                    _freePages = new HashSet<long>();

                    ms.Position = 0;

                    // this may cause additional pages to be freed, so we need need the while loop to track them all
                    _env.FreeSpace.Add(this, slice, ms);
                    ms.Position = 0; // so if we have additional freed pages, they will be added

                    if (_freePages.Count != 0)
                    {
                        _freePages.UnionWith(old);
                    }
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
                if (_rootTreeData == null)
                {
                    _rootTreeData = new TreeDataInTransaction(_env.Root)
                        {
                            Root = GetReadOnlyPage(_env.Root.State.RootPageNumber)
                        };
                }
                return _rootTreeData;
            }
            if (tree == _env.FreeSpace)
            {
                if (_fresSpaceTreeData == null)
                {
                    _fresSpaceTreeData = new TreeDataInTransaction(_env.FreeSpace)
                    {
                        Root = GetReadOnlyPage(_env.FreeSpace.State.RootPageNumber)
                    };
                }
                return _fresSpaceTreeData;
            }

            TreeDataInTransaction c;
            if (_treesInfo.TryGetValue(tree, out c))
            {
                return c;
            }
            c = new TreeDataInTransaction(tree)
                {
                    Root = GetReadOnlyPage(tree.State.RootPageNumber)
                };
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
    }
}