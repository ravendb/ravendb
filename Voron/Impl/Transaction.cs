using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Voron.Impl.FileHeaders;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl
{
    public unsafe class Transaction : IDisposable
    {
        private const int PagesTakenByHeader = 1;
        private readonly IVirtualPager _dataPager, _scratchPager;
        private readonly StorageEnvironment _env;
        private readonly long _id;

        private readonly WriteAheadJournal _journal;
        private long _lastAllocatedPageInScratch = 1;
        private Dictionary<Tuple<Tree, Slice>, Tree> _multiValueTrees;
        private readonly HashSet<long> _dirtyPages = new HashSet<long>();
        private readonly HashSet<PagerState> _pagerStates = new HashSet<PagerState>();
        private readonly IFreeSpaceHandling _freeSpaceHandling;

        private readonly Dictionary<long, long> _scratchPagesTable = new Dictionary<long, long>();

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
        private readonly StorageEnvironmentState _state;
        private int _allocatedPagesInTransaction;
        private int _overflowPagesInTransaction;
        private TransactionHeader* _txHeader;

        public Page TempPage
        {
            get { return _dataPager.TempPage; }
        }

        public bool Committed { get; private set; }

        public bool RolledBack { get; private set; }

        public PagerState LatestPagerState { get; private set; }

        public StorageEnvironmentState State
        {
            get { return _state; }
        }

        public IEnumerable<KeyValuePair<long, long>> PageTranslations { get { return _scratchPagesTable; } }

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

            _scratchPager = env.Options.ScratchPager;
            _scratchPager.PagerState.AddRef();
            _pagerStates.Add(_scratchPager.PagerState);

            _state = env.State.Clone();

            _journal.Files.ForEach(SetLogReference);

            InitTransactionHeader();

            MarkTreesForWriteTransaction();
        }

        private void InitTransactionHeader()
        {
            _scratchPager.EnsureContinuous(this, 0, (int) _env.Options.InitialLogFileSize / _scratchPager.PageSize);
            var ptr = _scratchPager.GetWritable(0).Base;
            NativeMethods.memset(ptr, 0, _scratchPager.PageSize);
            _txHeader = (TransactionHeader*)ptr;
            _txHeader->HeaderMarker = Constants.TransactionHeaderMarker;

            _txHeader->TransactionId = _id;
            _txHeader->NextPageNumber = _state.NextPageNumber;
            _txHeader->LastPageNumber = -1;
            _txHeader->PageCount = -1;
            _txHeader->Crc = 0;
            _txHeader->TxMarker = TransactionMarker.None;

            _allocatedPagesInTransaction = 0;
            _overflowPagesInTransaction = 0;
            _scratchPagesTable.Clear();
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

        public Page ModifyPage(long p, Cursor c)
        {
            Page page;
            if (_dirtyPages.Contains(p))
            {
                page = GetTableForModification(p, c);
                page.Dirty = true;

                return page;
            }

            page = GetTableForModification(p, c);

            var newPage = AllocatePage(1, p); // allocate new page in a log file but with the same number

            NativeMethods.memcpy(newPage.Base, page.Base, _dataPager.PageSize);
            newPage.LastSearchPosition = page.LastSearchPosition;
            newPage.LastMatch = page.LastMatch;

            return newPage;
        }

        private Page GetTableForModification(long p, Cursor c)
        {
            var page = c.GetPage(p);
            if (page != null)
                return page;

            long value;
            if (_scratchPagesTable.TryGetValue(p, out value))
            {
                return _scratchPager.Read(value);
            }
            return _journal.ReadPage(this, p) ?? _dataPager.Read(p);
        }

        public Page GetReadOnlyPage(long n)
        {
            long value;
            if (_scratchPagesTable.TryGetValue(n, out value))
                return _scratchPager.Read(value);

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

            _scratchPager.EnsureContinuous(this, _lastAllocatedPageInScratch, numberOfPages);

            var positionInScratch = _lastAllocatedPageInScratch;
            _lastAllocatedPageInScratch += numberOfPages;
            var page = _scratchPager.GetWritable(positionInScratch);

            page.PageNumber = pageNumber.Value;

            _allocatedPagesInTransaction ++;
            if (numberOfPages > 1)
            {
                _overflowPagesInTransaction += (numberOfPages - 1);
            }

            _scratchPagesTable[pageNumber.Value] = positionInScratch; 

            page.Lower = (ushort)Constants.PageHeaderSize;
            page.Upper = (ushort)_dataPager.PageSize;
            page.Dirty = true;

            _dirtyPages.Add(page.PageNumber);
            return page;
        }



        internal int GetNumberOfFreePages(NodeHeader* node)
        {
            return GetNodeDataSize(node) / Constants.PageNumberSize;
        }

        internal int GetNodeDataSize(NodeHeader* node)
        {
            if (node->Flags == (NodeFlags.PageRef)) // lots of data, enough to overflow!
            {
                var overflowPage = GetReadOnlyPage(node->PageNumber);
                return overflowPage.OverflowSize;
            }
            return node->DataSize;
        }

        public void Commit()
        {
            if (Flags != (TransactionFlags.ReadWrite) || RolledBack)
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

            _txHeader->LastPageNumber = _state.NextPageNumber - 1;
            _txHeader->PageCount = _allocatedPagesInTransaction;
            _txHeader->OverflowPageCount = _overflowPagesInTransaction;
            _state.Root.State.CopyTo(&_txHeader->Root);
            _state.FreeSpaceRoot.State.CopyTo(&_txHeader->FreeSpace);

            var crcOffset = PagesTakenByHeader * _scratchPager.PageSize;
            var crcCount = (_allocatedPagesInTransaction + _overflowPagesInTransaction) * _scratchPager.PageSize;

            _txHeader->Crc = Crc.Value(_scratchPager.PagerState.Base, crcOffset, crcCount);

            _txHeader->TxMarker |= TransactionMarker.Commit;

            _journal.WriteToJournal(this, _allocatedPagesInTransaction + _overflowPagesInTransaction + PagesTakenByHeader);

            _env.SetStateAfterTransactionCommit(State);
            Committed = true;
            AfterCommit(_id);
        }


        public void Rollback()
        {
            if (Committed || RolledBack || Flags != (TransactionFlags.ReadWrite))
                return;

            RolledBack = true;
            // there is nothing we need to do here
            // all the writes happened at the scratch location
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

        public void Dispose()
        {
            if (!Committed && !RolledBack && Flags == TransactionFlags.ReadWrite)
                Rollback();

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

        public Page GetFirstScratchPage()
        {
            return _scratchPager.Read(0);
        }
    }
}