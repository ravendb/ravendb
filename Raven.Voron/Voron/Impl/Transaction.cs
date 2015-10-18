using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Sparrow;
using Sparrow.Platform;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Impl.FileHeaders;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Voron.Trees;

namespace Voron.Impl
{
    public unsafe class LowLevelTransaction : IDisposable
    {
        private const int PagesTakenByHeader = 1;
        private readonly IVirtualPager _dataPager;
        private readonly StorageEnvironment _env;
        private readonly long _id;
        private Tree _root;

        internal Action<LowLevelTransaction> AfterCommit = delegate { };
        public bool FlushedToJournal { get; private set; }

        public Tree RootObjects
        {
            get { return _root; }
        }

        private readonly WriteAheadJournal _journal;
        private readonly HashSet<long> _dirtyPages = new HashSet<long>(NumericEqualityComparer.Instance);
        private readonly Dictionary<long, long> _dirtyOverflowPages = new Dictionary<long, long>(NumericEqualityComparer.Instance);
        private readonly HashSet<PagerState> _pagerStates = new HashSet<PagerState>();
        private readonly IFreeSpaceHandling _freeSpaceHandling;

        private int _allocatedPagesInTransaction;
        private int _overflowPagesInTransaction;
        private TransactionHeader* _txHeader;

        private PageFromScratchBuffer _transactionHeaderPage;
        private readonly HashSet<PageFromScratchBuffer> _transactionPages = new HashSet<PageFromScratchBuffer>();
        private readonly HashSet<long> _freedPages = new HashSet<long>();
        private readonly List<PageFromScratchBuffer> _unusedScratchPages = new List<PageFromScratchBuffer>();

        private readonly Dictionary<long, PageFromScratchBuffer> _scratchPagesTable = new Dictionary<long, PageFromScratchBuffer>(NumericEqualityComparer.Instance);
        internal readonly List<JournalSnapshot> JournalSnapshots = new List<JournalSnapshot>();

        private readonly StorageEnvironmentState _state;
        private readonly Dictionary<int, PagerState> _scratchPagerStates;

        public TransactionFlags Flags { get; private set; }

        internal bool CreatedByJournalApplicator;

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

        public bool Committed { get; private set; }

        public bool RolledBack { get; private set; }

        public StorageEnvironmentState State
        {
            get { return _state; }
        }

        public uint Crc
        {
            get { return _txHeader->Crc; }
        }



        public LowLevelTransaction(StorageEnvironment env, long id, TransactionFlags flags, IFreeSpaceHandling freeSpaceHandling)
        {
            _dataPager = env.Options.DataPager;
            _env = env;
            _journal = env.Journal;
            _id = id;
            _freeSpaceHandling = freeSpaceHandling;
            Flags = flags;
            var scratchPagerStates = env.ScratchBufferPool.GetPagerStatesOfAllScratches();

            foreach (var scratchPagerState in scratchPagerStates.Values)
            {
                scratchPagerState.AddRef();
                _pagerStates.Add(scratchPagerState);
            }


            if (flags.HasFlag(TransactionFlags.ReadWrite) == false)
            {
                // for read transactions, we need to keep the pager state frozen
                // for write transactions, we can use the current one (which == null)
                _scratchPagerStates = scratchPagerStates;

                _state = env.State.Clone();

                InitializeRoots();

                foreach (var snapshot in _journal.GetSnapshots())
                    AddJournalSnapshot(snapshot);

                return;
            }

            _state = env.State.Clone();
            InitializeRoots();
            InitTransactionHeader();
        }
        internal void UpdateRootsIfNeeded(Tree root)
        {
            //can only happen during initial transaction that creates Root and FreeSpaceRoot trees
            if (State.Root != null)
                return;

            State.Root = root.State;

            _root = root;
        }

        private void InitializeRoots()
        {
            if (_state.Root != null)
            {
                _state.Root.InWriteTransaction = Flags == TransactionFlags.ReadWrite;
                _root = new Tree(this, null, _state.Root) { Name = Constants.RootTreeName };
            }
        }

        private void InitTransactionHeader()
        {
            var allocation = _env.ScratchBufferPool.Allocate(this, 1);
            var page = _env.ScratchBufferPool.ReadPage(allocation.ScratchFileNumber, allocation.PositionInScratchBuffer);

            _transactionHeaderPage = allocation;

            UnmanagedMemory.Set(page.Pointer, 0, Environment.Options.PageSize);
            _txHeader = (TransactionHeader*)page.Pointer;
            _txHeader->HeaderMarker = Constants.TransactionHeaderMarker;

            _txHeader->TransactionId = _id;
            _txHeader->NextPageNumber = _state.NextPageNumber;
            _txHeader->LastPageNumber = -1;
            _txHeader->PageCount = -1;
            _txHeader->Crc = 0;
            _txHeader->TxMarker = TransactionMarker.None;
            _txHeader->Compressed = false;
            _txHeader->CompressedSize = 0;
            _txHeader->UncompressedSize = 0;

            _allocatedPagesInTransaction = 0;
            _overflowPagesInTransaction = 0;

            _scratchPagesTable.Clear();
        }

        private void AddJournalSnapshot(JournalSnapshot snapshot)
        {
            if (JournalSnapshots.Any(x => x.Number == snapshot.Number))
                throw new InvalidOperationException("Cannot add a snapshot of log file with number " + snapshot.Number +
                                                    " to the transaction, because it already exists in a snapshot collection");

            JournalSnapshots.Add(snapshot);
        }

        internal PageFromScratchBuffer GetTransactionHeaderPage()
        {
            return this._transactionHeaderPage;
        }

        internal HashSet<PageFromScratchBuffer> GetTransactionPages()
        {
            return _transactionPages;
        }

        internal List<PageFromScratchBuffer> GetUnusedScratchPages()
        {
            return _unusedScratchPages;
        }

        internal HashSet<long> GetFreedPagesNumbers()
        {
            return _freedPages;
        }


        internal Page ModifyPage(long num)
        {
            _env.AssertFlushingNotFailed();

            var currentPage = GetPage(num);
            if (_dirtyPages.Contains(num))
            {
                return currentPage;
            }

            var newPage = AllocatePage(1, num); // allocate new page in a log file but with the same number
            Memory.Copy(newPage.Pointer, currentPage.Pointer, Environment.Options.PageSize);

            return newPage;
        }

        private const int InvalidScratchFile = -1;
        private PagerStateCacheItem lastScratchFileUsed = new PagerStateCacheItem(InvalidScratchFile, null);
        private bool _disposed;

        public Page GetPage(long pageNumber)
        {
            if (_disposed)
                throw new ObjectDisposedException("Transaction");
            Page p;

            PageFromScratchBuffer value;
            if (_scratchPagesTable.TryGetValue(pageNumber, out value))
            {
                PagerState state = null;
                if (_scratchPagerStates != null)
                {
                    var lastUsed = lastScratchFileUsed;
                    if (lastUsed.FileNumber == value.ScratchFileNumber)
                    {
                        state = lastUsed.State;
                    }
                    else
                    {
                        state = _scratchPagerStates[value.ScratchFileNumber];
                        lastScratchFileUsed = new PagerStateCacheItem(value.ScratchFileNumber, state);
                    }
                }

                p = _env.ScratchBufferPool.ReadPage(value.ScratchFileNumber, value.PositionInScratchBuffer, state);
            }
            else
            {
                p = _journal.ReadPage(this, pageNumber, _scratchPagerStates) ?? _dataPager.ReadPage(pageNumber);
            }

            Debug.Assert(p != null && p.PageNumber == pageNumber, string.Format("Requested ReadOnly page #{0}. Got #{1} from {2}", pageNumber, p.PageNumber, p.Source));

            return p;
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
            return AllocatePage(numberOfPages, pageNumber.Value);
        }

        private Page AllocatePage(int numberOfPages, long pageNumber)
        {
            if (_disposed)
                throw new ObjectDisposedException("Transaction");

            if (_env.Options.MaxStorageSize.HasValue) // check against quota
            {
                var maxAvailablePageNumber = _env.Options.MaxStorageSize / Environment.Options.PageSize;

                if (pageNumber > maxAvailablePageNumber)
                    throw new QuotaException(
                        string.Format(
                            "The maximum storage size quota ({0} bytes) has been reached. " +
                            "Currently configured storage quota is allowing to allocate the following maximum page number {1}, while the requested page number is {2}. " +
                            "To increase the quota, use the MaxStorageSize property on the storage environment options.",
                            _env.Options.MaxStorageSize, maxAvailablePageNumber, pageNumber));
            }


            Debug.Assert(pageNumber < State.NextPageNumber);

            var pageFromScratchBuffer = _env.ScratchBufferPool.Allocate(this, numberOfPages);
            _transactionPages.Add(pageFromScratchBuffer);

            _allocatedPagesInTransaction++;
            if (numberOfPages > 1)
            {
                _overflowPagesInTransaction += (numberOfPages - 1);
            }

            _scratchPagesTable[pageNumber] = pageFromScratchBuffer;

            _dirtyPages.Add(pageNumber);

            if (numberOfPages > 1)
                _dirtyOverflowPages.Add(pageNumber + 1, numberOfPages - 1);

            var newPage = _env.ScratchBufferPool.ReadPage(pageFromScratchBuffer.ScratchFileNumber,
                pageFromScratchBuffer.PositionInScratchBuffer);
            newPage.PageNumber = pageNumber;
            newPage.Flags = PageFlags.Single;
            return newPage;

        }


        public bool IsDisposed
        {
            get { return _disposed; }
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            if (!Committed && !RolledBack && Flags == TransactionFlags.ReadWrite)
                Rollback();

            _disposed = true;

            _env.TransactionCompleted(this);
            foreach (var pagerState in _pagerStates)
            {
                pagerState.Release();
            }
        }

        internal void FreePage(long pageNumber)
        {
            if (_disposed)
                throw new ObjectDisposedException("Transaction");

            Debug.Assert(pageNumber >= 0);
            _freeSpaceHandling.FreePage(this, pageNumber);

            _freedPages.Add(pageNumber);

            PageFromScratchBuffer scratchPage;
            if (_scratchPagesTable.TryGetValue(pageNumber, out scratchPage))
            {
                _transactionPages.Remove(scratchPage);
                _unusedScratchPages.Add(scratchPage);

                _scratchPagesTable.Remove(pageNumber);
            }

            long numberOfOverflowPages;

            if (_dirtyPages.Remove(pageNumber))
            {
                _allocatedPagesInTransaction--;
            }
            else if (_dirtyOverflowPages.TryGetValue(pageNumber, out numberOfOverflowPages))
            {
                _overflowPagesInTransaction--;

                _dirtyOverflowPages.Remove(pageNumber);

                if (numberOfOverflowPages > 1) // prevent adding range which length is 0
                    _dirtyOverflowPages.Add(pageNumber + 1, numberOfOverflowPages - 1); // change the range of the overflow page
            }
        }


        private class PagerStateCacheItem
        {
            public readonly int FileNumber;
            public readonly PagerState State;

            public PagerStateCacheItem(int file, PagerState state)
            {
                this.FileNumber = file;
                this.State = state;
            }
        }


        public void Commit()
        {
            if (_disposed)
                throw new ObjectDisposedException("Transaction");

            if (Flags != (TransactionFlags.ReadWrite))
                return; // nothing to do

            if (Committed)
                throw new InvalidOperationException("Cannot commit already committed transaction.");

            if (RolledBack)
                throw new InvalidOperationException("Cannot commit rolled-back transaction.");

     
            _txHeader->LastPageNumber = _state.NextPageNumber - 1;
            _txHeader->PageCount = _allocatedPagesInTransaction;
            _txHeader->OverflowPageCount = _overflowPagesInTransaction;
            _state.Root.CopyTo(&_txHeader->Root);

            _txHeader->TxMarker |= TransactionMarker.Commit;

            if (_allocatedPagesInTransaction + _overflowPagesInTransaction > 0) // nothing changed in this transaction
            {
                _journal.WriteToJournal(this, _allocatedPagesInTransaction + _overflowPagesInTransaction + PagesTakenByHeader);
                FlushedToJournal = true;
            }

            // release scratch file page allocated for the transaction header
            _env.ScratchBufferPool.Free(_transactionHeaderPage.ScratchFileNumber, _transactionHeaderPage.PositionInScratchBuffer, -1);

            Committed = true;
            AfterCommit(this);
        }


        public void Rollback()
        {
            if (_disposed)
                throw new ObjectDisposedException("Transaction");


            if (Committed || RolledBack || Flags != (TransactionFlags.ReadWrite))
                return;

            foreach (var pageFromScratch in _transactionPages)
            {
                _env.ScratchBufferPool.Free(pageFromScratch.ScratchFileNumber, pageFromScratch.PositionInScratchBuffer, -1);
            }

            foreach (var pageFromScratch in _unusedScratchPages)
            {
                _env.ScratchBufferPool.Free(pageFromScratch.ScratchFileNumber, pageFromScratch.PositionInScratchBuffer, -1);
            }

            // release scratch file page allocated for the transaction header
            _env.ScratchBufferPool.Free(_transactionHeaderPage.ScratchFileNumber, _transactionHeaderPage.PositionInScratchBuffer, -1);

            RolledBack = true;
        }


        internal LowLevelTransaction JournalApplicatorTransaction()
        {
            CreatedByJournalApplicator = true;
            return this;
        }


        internal void AddPagerState(PagerState state)
        {
            if (state == null)
                return;

            state.AddRef();
            _pagerStates.Add(state);
        }
    }

    public unsafe class Transaction : IDisposable
    {
        private Dictionary<Tuple<Tree, Slice>, Tree> _multiValueTrees;
        private readonly LowLevelTransaction _lowLevelTransaction;

        public LowLevelTransaction LowLevelTransaction
        {
            get { return _lowLevelTransaction; }
        }

        internal Action<Transaction> AfterCommit = delegate { };

        private readonly Dictionary<string, Tree> _trees = new Dictionary<string, Tree>();


        public Transaction(LowLevelTransaction lowLevelTransaction)
        {
            _lowLevelTransaction = lowLevelTransaction;
        }


        public Tree ReadTree(string treeName)
        {
            Tree tree;
            if (_trees.TryGetValue(treeName, out tree))
                return tree;

            var header = (TreeRootHeader*)_lowLevelTransaction.RootObjects.DirectRead((Slice)treeName);
            if (header != null)
            {
                tree = Tree.Open(_lowLevelTransaction, this, header);
                tree.Name = treeName;
                _trees.Add(treeName, tree);
                return tree;
            }

            _trees.Add(treeName, null);
            return null;
        }

      
        public IEnumerable<Tree> Trees
        {
            get { return _trees.Values; }
        }

        public void Commit()
        {
            if (_multiValueTrees != null)
            {
                foreach (var multiValueTree in _multiValueTrees)
                {
                    var parentTree = multiValueTree.Key.Item1;
                    var key = multiValueTree.Key.Item2;
                    var childTree = multiValueTree.Value;

                    var trh = (TreeRootHeader*)parentTree.DirectAdd(key, sizeof(TreeRootHeader), TreeNodeFlags.MultiValuePageRef);
                    childTree.State.CopyTo(trh);

                    //parentTree.SetAsMultiValueTreeRef(this, key);
                }
            }

            foreach (var tree in Trees)
            {
                if (tree == null)
                    continue;
                tree.State.InWriteTransaction = false;
                var treeState = tree.State;
                if (treeState.IsModified)
                {
                    var treePtr = (TreeRootHeader*)_lowLevelTransaction.RootObjects.DirectAdd((Slice)tree.Name, sizeof(TreeRootHeader));
                    treeState.CopyTo(treePtr);
                }
            }
            _lowLevelTransaction.Commit();
        }


        internal void AddMultiValueTree(Tree tree, Slice key, Tree mvTree)
        {
            if (_multiValueTrees == null)
                _multiValueTrees = new Dictionary<Tuple<Tree, Slice>, Tree>(new TreeAndSliceComparer());
            mvTree.IsMultiValueTree = true;
            _multiValueTrees.Add(Tuple.Create(tree, key), mvTree);
        }

        internal bool TryGetMultiValueTree(Tree tree, Slice key, out Tree mvTree)
        {
            mvTree = null;
            if (_multiValueTrees == null)
                return false;
            return _multiValueTrees.TryGetValue(Tuple.Create(tree, key), out mvTree);
        }

        internal bool TryRemoveMultiValueTree(Tree parentTree, Slice key)
        {
            var keyToRemove = Tuple.Create(parentTree, key);
            if (_multiValueTrees == null || !_multiValueTrees.ContainsKey(keyToRemove))
                return false;

            return _multiValueTrees.Remove(keyToRemove);
        }


        internal void AddTree(string name, Tree tree)
        {
            Tree value;
            if (_trees.TryGetValue(name, out value) && value != null)
            {
                throw new InvalidOperationException("Tree already exists: " + name);
            }
            _trees[name] = tree;
        }



        public void DeleteTree(string name)
        {
            if (_lowLevelTransaction.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot create a new newRootTree with a read only transaction");

            Tree tree = ReadTree(name);
            if (tree == null)
                return;

            foreach (var page in tree.AllPages())
            {
                _lowLevelTransaction.FreePage(page);
            }

            _lowLevelTransaction.RootObjects.Delete((Slice)name);

            _trees.Remove(name);
        }

        public void RenameTree(string fromName, string toName)
        {
            if (_lowLevelTransaction.Flags == (TransactionFlags.ReadWrite) == false)
                throw new ArgumentException("Cannot rename a new tree with a read only transaction");

            if (toName.Equals(Constants.RootTreeName, StringComparison.InvariantCultureIgnoreCase))
                throw new InvalidOperationException("Cannot create a tree with reserved name: " + toName);

            if (ReadTree(toName) != null)
                throw new ArgumentException("Cannot rename a tree with the name of an existing tree: " + toName);

            Tree fromTree = ReadTree(fromName);
            if (fromTree == null)
                throw new ArgumentException("Tree " + fromName + " does not exists");

            Slice key = (Slice)toName;

            _lowLevelTransaction.RootObjects.Delete((Slice)fromName);
            var ptr = _lowLevelTransaction.RootObjects.DirectAdd(key, sizeof(TreeRootHeader));
            fromTree.State.CopyTo((TreeRootHeader*)ptr);
            fromTree.Name = toName;
            fromTree.State.IsModified = true;

            _trees.Remove(fromName);
            _trees.Remove(toName);

            AddTree(toName, fromTree);
        }

        public unsafe Tree CreateTree(string name)
        {
            Tree tree = ReadTree(name);
            if (tree != null)
                return tree;

            if (_lowLevelTransaction.Flags == (TransactionFlags.ReadWrite) == false)
                throw new InvalidOperationException("No such tree: " + name + " and cannot create trees in read transactions");

            Slice key = name;

            tree = Tree.Create(_lowLevelTransaction, this);
            tree.Name = name;
            var space = _lowLevelTransaction.RootObjects.DirectAdd(key, sizeof(TreeRootHeader));

            tree.State.CopyTo((TreeRootHeader*)space);
            tree.State.IsModified = true;
            AddTree(name, tree);

            return tree;
        }


        public void Dispose()
        {
            if (_lowLevelTransaction != null)
                _lowLevelTransaction.Dispose();
        }
    }

    public static class TransactionLegacyExtensions
    {
        public static TreePage GetReadOnlyTreePage(this LowLevelTransaction tx, long pageNumber)
        {
            return tx.GetPage(pageNumber).ToTreePage();
        }
    }
}
