using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Voron.Impl.FileHeaders;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl
{
    public class Transaction : IDisposable
    {
        public long NextPageNumber
        {
            get
            {
                return _state.NextPageNumber;
            }
        }

        private IVirtualPager _pager;
        private StorageEnvironment _env;
        private long _id;

        private Dictionary<Tuple<Tree, Slice>, Tree> _multiValueTrees;
        private readonly Dictionary<long, long> _dirtyPages = new Dictionary<long, long>();
        private readonly List<long> _freedPages = new List<long>();
        private readonly HashSet<PagerState> _pagerStates = new HashSet<PagerState>();
        private IFreeSpaceRepository _freeSpaceRepository;

        private List<string> _deletedTrees;

        public TransactionFlags Flags { get; private set; }

        internal StorageEnvironment Environment
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

        internal Action<long> AfterCommit = delegate { };
        private StorageEnvironmentState _state;

        public Page TempPage
        {
            get { return _pager.TempPage; }
        }

        public bool Committed { get; private set; }

        public PagerState LatestPagerState { get; private set; }

        public StorageEnvironmentState State
        {
            get { return _state; }
        }


        public Transaction(IVirtualPager pager, StorageEnvironment env, long id, TransactionFlags flags, IFreeSpaceRepository freeSpaceRepository)
        {
            _pager = pager;
            _env = env;
            _id = id;
            _freeSpaceRepository = freeSpaceRepository;
            Flags = flags;

            if (flags.HasFlag(TransactionFlags.ReadWrite) == false)
            {
                _state = env.State;
                return;
            }

            _state = env.State.Clone();
            MarkTreesForWriteTransaction();
        }

        private void MarkTreesForWriteTransaction()
        {
            if (_state.Root != null)
                _state.Root.State.InWriteTransaction = true;
            if (_state.FreeSpaceRoot != null)
                _state.FreeSpaceRoot.State.InWriteTransaction = true;
            foreach (var tree in _state.Trees.Values)
            {
                tree.State.InWriteTransaction = true;
            }
        }

        public Tree GetTree(string treeName)
        {
            return State.GetTree(treeName, this);
        }

        public Page ModifyCursor(Tree tree, Cursor c)
        {
            Debug.Assert(c.Pages.Count > 0); // cannot modify an empty cursor

            var node = c.Pages.Last;
            while (node != null)
            {
                var parent = node.Next != null ? node.Next.Value : null;
                c.Update(node, ModifyPage(tree, parent, node.Value.PageNumber, c));
                node = node.Previous;
            }

            tree.State.RootPageNumber = c.Pages.Last.Value.PageNumber;

            return c.Pages.First.Value;
        }

        public unsafe Page ModifyPage(Tree tree, Page parent, long p, Cursor c)
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
            Page page = _freeSpaceRepository.TryAllocateFromFreeSpace(this, num);
            if (page == null) // allocate from end of file
            {
                if (num > 1)
                    _pager.EnsureContinious(this, NextPageNumber, num);
                page = _pager.Get(this, NextPageNumber);
                page.PageNumber = NextPageNumber;
                _state.NextPageNumber += num;
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
            if (node->Flags == (NodeFlags.PageRef)) // lots of data, enough to overflow!
            {
                var overflowPage = GetReadOnlyPage(node->PageNumber);
                return overflowPage.OverflowSize;
            }
            return node->DataSize;
        }

        public unsafe void Commit()
        {
            if (Flags != (TransactionFlags.ReadWrite))
                return; // nothing to do

            FlushAllMultiValues();
            _freeSpaceRepository.FlushFreeState(this);

            if (_deletedTrees != null)
            {
                foreach (var deletedTree in _deletedTrees)
                {
                    State.RemoveTree(deletedTree);
                }
            }

            State.Root.State.InWriteTransaction = false;
            State.FreeSpaceRoot.State.InWriteTransaction = false;

            foreach (var treeKvp in State.Trees)
            {
                treeKvp.Value.State.InWriteTransaction = false;
                var treeState = treeKvp.Value.State;
                if (treeState.IsModified)
                {
                    var treePtr = (TreeRootHeader*)State.Root.DirectAdd(this, treeKvp.Key, sizeof(TreeRootHeader));
                    treeState.CopyTo(treePtr);
                }
            }


#if DEBUG
            if (State.Root != null && State.FreeSpaceRoot != null)
            {
                Debug.Assert(State.Root.State.RootPageNumber != State.FreeSpaceRoot.State.RootPageNumber);
            }
#endif
            // Because we don't know in what order the OS will flush the pages 
            // we need to do this twice, once for the data, and then once for the metadata

            var sortedPagesToFlush = _dirtyPages.Select(x => x.Value).Distinct().ToList();
            sortedPagesToFlush.Sort();
            _pager.Flush(sortedPagesToFlush);

            if (_freeSpaceRepository != null)
                _freeSpaceRepository.LastTransactionPageUsage(sortedPagesToFlush.Count);

            Page relevantPage = _pager.Get(this, _id & 1);
            WriteHeader(relevantPage); // this will cycle between the first and second pages

            _pager.Flush(_id & 1); // and now we flush the metadata as well

            _pager.Sync();

            _env.SetStateAfterTransactionCommit(State);

            Committed = true;

            AfterCommit(_id);
        }

        private unsafe void FlushAllMultiValues()
        {
            if (_multiValueTrees == null)
                return;

            foreach (var multiValueTree in _multiValueTrees)
            {
                var parentTree = multiValueTree.Key.Item1;
                var key = multiValueTree.Key.Item2;
                var childTree = multiValueTree.Value;

                var trh = (TreeRootHeader*)parentTree.DirectAdd(this, key, sizeof(TreeRootHeader));
                childTree.State.CopyTo(trh);

                parentTree.SetAsMultiValueTreeRef(this, key);
            }
        }

        private unsafe void WriteHeader(Page pg)
        {
            var fileHeader = (FileHeader*)pg.Base;
            fileHeader->TransactionId = _id;
            fileHeader->LastPageNumber = NextPageNumber - 1;

            State.FreeSpaceRoot.State.CopyTo(&fileHeader->FreeSpace);
            State.Root.State.CopyTo(&fileHeader->Root);
        }

        private void FlushFreePages()
        {
            int iterationCounter = 0;
            while (_freedPages.Count != 0)
            {
                Slice slice = string.Format("tx/{0:0000000000000000000}/{1}", _id, iterationCounter);

                iterationCounter++;
                using (var ms = new MemoryStream())
                using (var binaryWriter = new BinaryWriter(ms))
                {
                    _freeSpaceRepository.RegisterFreePages(slice, _id, _freedPages);
                    foreach (var freePage in _freedPages)
                    {
                        binaryWriter.Write(freePage);
                    }
                    _freedPages.Clear();

                    ms.Position = 0;

                    // this may cause additional pages to be freed, so we need need the while loop to track them all
                    State.FreeSpaceRoot.Add(this, slice, ms);
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

        public void FreePage(long pageNumber)
        {
            _dirtyPages.Remove(pageNumber);
#if DEBUG
            Debug.Assert(pageNumber >= 2 && pageNumber <= _pager.NumberOfAllocatedPages);
            Debug.Assert(_freedPages.Contains(pageNumber) == false);
#endif
            _freedPages.Add(pageNumber);
        }

        internal void UpdateRootsIfNeeded(Tree root, Tree freeSpace)
        {
            //can only happen during initial transaction that creates Root and FreeSpaceRoot trees
            if (State.Root == null && State.FreeSpaceRoot == null && State.Trees.Count == 0)
            {
                State.Root = root;
                State.FreeSpaceRoot = freeSpace;
            }
        }

        public void AddPagerState(PagerState state)
        {
            LatestPagerState = state;
            _pagerStates.Add(state);
        }

        public Cursor NewCursor(Tree tree)
        {
            return new Cursor();
        }

        public unsafe void AddMultiValueTree(Tree tree, Slice key, Tree mvTree)
        {
            if (_multiValueTrees == null)
                _multiValueTrees = new Dictionary<Tuple<Tree, Slice>, Tree>(new TreeAndSliceComparer(_env.SliceComparer));
            mvTree.IsMultiValueTree = true;
            _multiValueTrees.Add(Tuple.Create(tree, key), mvTree);
        }

        public bool TryGetMultiValueTree(Tree tree, Slice key, out Tree mvTree)
        {
            mvTree = null;
            if (_multiValueTrees == null)
                return false;
            return _multiValueTrees.TryGetValue(Tuple.Create(tree, key), out mvTree);
        }

        public bool TryRemoveMultiValueTree(Tree parentTree, Slice key)
        {
            var keyToRemove = Tuple.Create(parentTree, key);
            if (_multiValueTrees == null || !_multiValueTrees.ContainsKey(keyToRemove))
                return false;

            return _multiValueTrees.Remove(keyToRemove);
        }

        public void DeletedTree(string name)
        {
            if (_deletedTrees == null)
                _deletedTrees = new List<string>();
            _deletedTrees.Add(name);
        }
    }
}