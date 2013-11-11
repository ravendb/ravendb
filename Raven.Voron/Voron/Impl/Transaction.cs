using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Voron.Impl.FileHeaders;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Voron.Trees;

namespace Voron.Impl
{
	using System.Collections;

	public class Transaction : IDisposable
    {
        private readonly IVirtualPager _dataPager;
        private readonly StorageEnvironment _env;
        private readonly long _id;

        private readonly WriteAheadJournal _journal;
        private Dictionary<Tuple<Tree, Slice>, Tree> _multiValueTrees;
        private readonly HashSet<long> _dirtyPages = new HashSet<long>();
        private readonly HashSet<PagerState> _pagerStates = new HashSet<PagerState>();
        private readonly IFreeSpaceHandling _freeSpaceHandling;

        internal readonly List<LogSnapshot> LogSnapshots = new List<LogSnapshot>();
        private readonly List<Action> _releaseLogActions = new List<Action>(); 

        private List<string> _deletedTrees;

        public TransactionFlags Flags { get; private set; }

        internal StorageEnvironment Environment
        {
            get { return _env; }
        }

        public IVirtualPager DataPager
        {
            get { return _dataPager; }
        }

        public long Id
        {
            get { return _id; }
        }

        internal Action<long> AfterCommit = delegate { };
        private StorageEnvironmentState _state;

        public Page TempPage
        {
            get { return _dataPager.TempPage; }
        }

        public bool Committed { get; private set; }

        public PagerState LatestPagerState { get; private set; }

        public StorageEnvironmentState State
        {
            get { return _state; }
        }


        public Transaction(StorageEnvironment env, long id, TransactionFlags flags, IFreeSpaceHandling freeSpaceHandling)
        {
            _dataPager = env.Options.DataPager;
            _env = env;
            _journal = env.Journal;
            _id = id;
            _freeSpaceHandling = freeSpaceHandling;
            Flags = flags;

            if (flags.HasFlag(TransactionFlags.ReadWrite) == false)
            {
                _state = env.State;
                _journal.GetSnapshots().ForEach(AddLogSnapshot);
                return;
            }

            _state = env.State.Clone();

            _journal.Files.ForEach(SetLogReference);
            _journal.TransactionBegin(this);
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

        public unsafe Page ModifyPage(long p, Cursor c)
        {
            Page page;
            if (_dirtyPages.Contains(p))
            {
                page = c.GetPage(p) ?? _journal.ReadPage(this, p);
                page.Dirty = true;

                return page;
            }

            page = c.GetPage(p) ?? _journal.ReadPage(this, p) ?? _dataPager.Read(p);

            var newPage = AllocatePage(1, p); // allocate new page in a log file but with the same number

            NativeMethods.memcpy(newPage.Base, page.Base, _dataPager.PageSize);
            newPage.LastSearchPosition = page.LastSearchPosition;
            newPage.LastMatch = page.LastMatch;

            return newPage;
        }

        public Page GetReadOnlyPage(long n)
        {
            return _journal.ReadPage(this, n) ?? _dataPager.Read(n);
        }

        public Page AllocatePage(int numberOfPages, long? pageNumber = null)
        {
            if (pageNumber == null)
            {
                pageNumber = _freeSpaceHandling.TryAllocateFromFreeSpace(this, numberOfPages);
                if (pageNumber == null) // allocate from end of file
                {
                    pageNumber = State.NextPageNumber;
                    State.NextPageNumber += numberOfPages;
                }
            }

            Debug.Assert(pageNumber < State.NextPageNumber);

            var page = _journal.Allocate(this, pageNumber.Value, numberOfPages);
            page.PageNumber = pageNumber.Value;
            page.Lower = (ushort)Constants.PageHeaderSize;
            page.Upper = (ushort)_dataPager.PageSize;
            page.Dirty = true;

            _dirtyPages.Add(page.PageNumber);
            return page;
        }

		public LinkedList<Tuple<Page, int>> AllocatePagesForOverflow(int numberOfPages)
		{
			var pageNumber = State.NextPageNumber;
			State.NextPageNumber += numberOfPages;

			var results = new LinkedList<Tuple<Page, int>>();

			var pages = _journal.AllocateForOverflow(this, pageNumber, numberOfPages);
			foreach (var item in pages)
			{
				var page = item.Item1;
				var allocatedPages = item.Item2;

				page.PageNumber = pageNumber;
				page.Lower = (ushort)Constants.PageHeaderSize;
				page.Upper = (ushort)_dataPager.PageSize;
				page.Dirty = true;

				_dirtyPages.Add(page.PageNumber);

				results.AddLast(new Tuple<Page, int>(page, _dataPager.PageSize * allocatedPages));

				pageNumber += allocatedPages;
			}

			return results;
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
                    var ptr = State.Root.DirectAdd(this, treeKvp.Key, sizeof(TreeRootHeader));
					Debug.Assert(ptr.Count == 1);
	                var treePtr = (TreeRootHeader*)ptr.FirstPointer;
                    treeState.CopyTo(treePtr);
                }
            }

#if DEBUG
            if (State.Root != null && State.FreeSpaceRoot != null)
            {
                Debug.Assert(State.Root.State.RootPageNumber != State.FreeSpaceRoot.State.RootPageNumber);
            }
#endif
            _journal.TransactionCommit(this);
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

                var ptr = parentTree.DirectAdd(this, key, sizeof(TreeRootHeader));
				Debug.Assert(ptr.Count == 1);

				var trh = (TreeRootHeader*)ptr.FirstPointer;
                childTree.State.CopyTo(trh);

                parentTree.SetAsMultiValueTreeRef(this, key);
            }
        }

        public void Dispose()
        {
            _env.TransactionCompleted(_id);
            foreach (var pagerState in _pagerStates)
            {
                pagerState.Release();
            }

            foreach (var releaseLog in _releaseLogActions)
            {
                releaseLog();
            }
        }

        public void FreePage(long pageNumber)
        {
            Debug.Assert(pageNumber >= 2);
            _dirtyPages.Remove(pageNumber);
            _freeSpaceHandling.FreePage(this, pageNumber);
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


        private void AddLogSnapshot(LogSnapshot snapshot)
        {
            if (LogSnapshots.Any(x => x.File.Number == snapshot.File.Number))
                throw new InvalidOperationException("Cannot add a snapshot of log file with number " + snapshot.File.Number +
                                                    " to the transaction, because it already exists in a snapshot collection");

            LogSnapshots.Add(snapshot);
            SetLogReference(snapshot.File);
        }

        public void SetLogReference(JournalFile journal)
        {
            journal.AddRef();
            _releaseLogActions.Add(journal.Release);
        }
    }
}