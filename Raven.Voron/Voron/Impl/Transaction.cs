using Sparrow;
using Sparrow.Platform;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Impl.FileHeaders;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl
{
    public unsafe class Transaction : IDisposable
    {
        private const int PagesTakenByHeader = 1;
        private readonly IVirtualPager _dataPager;
        private readonly StorageEnvironment _env;
        private readonly long _id;

        private readonly WriteAheadJournal _journal;
        private Dictionary<Tuple<Tree, Slice>, Tree> _multiValueTrees;
        private readonly HashSet<long> _dirtyPages = new HashSet<long>(NumericEqualityComparer.Instance);
        private readonly Dictionary<long, long> _dirtyOverflowPages = new Dictionary<long, long>(NumericEqualityComparer.Instance);
        private readonly HashSet<PagerState> _pagerStates = new HashSet<PagerState>();
        private readonly IFreeSpaceHandling _freeSpaceHandling;

        private readonly Dictionary<long, PageFromScratchBuffer> _scratchPagesTable = new Dictionary<long, PageFromScratchBuffer>(NumericEqualityComparer.Instance);

        internal readonly List<JournalSnapshot> JournalSnapshots = new List<JournalSnapshot>();

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

        public Tree FreeSpaceRoot
        {
            get { return _freeSpace; }
        }
        public Tree Root
        {
            get { return _root; }
        }

        internal Action<Transaction> AfterCommit = delegate { };
        internal Action<Transaction, DebugActionType> RecordTransactionState = delegate { };
        internal bool CreatedByJournalApplicator;
        private readonly StorageEnvironmentState _state;
        private Tree _root, _freeSpace;
       
        private int _allocatedPagesInTransaction;
        private int _overflowPagesInTransaction;
        private TransactionHeader* _txHeader;

        private PageFromScratchBuffer _transactionHeaderPage;
        private readonly HashSet<PageFromScratchBuffer> _transactionPages = new HashSet<PageFromScratchBuffer>();
        private readonly HashSet<long> _freedPages = new HashSet<long>();
        private readonly Stack<long> _pagesToFreeOnCommit = new Stack<long>();
        private readonly List<PageFromScratchBuffer> _unusedScratchPages = new List<PageFromScratchBuffer>();
        private readonly Dictionary<string, Tree> _trees = new Dictionary<string, Tree>();
        private readonly Dictionary<int, PagerState> _scratchPagerStates;

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

        public bool FlushedToJournal { get; private set; }

        public Transaction(StorageEnvironment env, long id, TransactionFlags flags, IFreeSpaceHandling freeSpaceHandling)
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

            MarkTreesForWriteTransaction();
        }

        private void InitializeRoots()
        {
            if (_state.Root != null)
            {
                _state.Root.InWriteTransaction = Flags == TransactionFlags.ReadWrite;
                _root = new Tree(this, _state.Root) {Name = Constants.RootTreeName};
            }
            if (_state.FreeSpaceRoot != null)
            {
                _state.FreeSpaceRoot.InWriteTransaction = Flags == TransactionFlags.ReadWrite;
                _freeSpace = new Tree(this, _state.FreeSpaceRoot) {Name = Constants.FreeSpaceTreeName, IsFreeSpaceTree = true };
            }
        }

        internal void WriteDirect(TransactionHeader* transactionHeader, PageFromScratchBuffer pages)
        {
            for (int i = 0; i < pages.NumberOfPages; i++)
            {
                var page = _env.ScratchBufferPool.ReadPage(this, pages.ScratchFileNumber, pages.PositionInScratchBuffer+i);
                int numberOfPages = 1;
                if (page.IsOverflow)
                {
                    numberOfPages = (page.OverflowSize / AbstractPager.PageSize) + (page.OverflowSize % AbstractPager.PageSize == 0 ? 0 : 1);
                    i += numberOfPages;
                    _overflowPagesInTransaction += (numberOfPages - 1);
                }

                WritePageDirect(page, numberOfPages);

                _state.NextPageNumber = transactionHeader->NextPageNumber;
            }
        }

        private void WritePageDirect(Page page, int numberOfPagesIncludingOverflow)
        {
            var pageFromScratchBuffer = _env.ScratchBufferPool.Allocate(this, numberOfPagesIncludingOverflow);

            var dest = _env.ScratchBufferPool.AcquirePagePointer(this, pageFromScratchBuffer.ScratchFileNumber, pageFromScratchBuffer.PositionInScratchBuffer);
            Memory.Copy(dest, page.Base, numberOfPagesIncludingOverflow * AbstractPager.PageSize);

            _allocatedPagesInTransaction++;

            _dirtyPages.Add(page.PageNumber);
            page.Dirty = true;

            if (numberOfPagesIncludingOverflow > 1)
                _dirtyOverflowPages.Add(page.PageNumber + 1, numberOfPagesIncludingOverflow - 1);

            _scratchPagesTable[page.PageNumber] = pageFromScratchBuffer;
            _transactionPages.Add(pageFromScratchBuffer);
        }

        private void InitTransactionHeader()
        {
            var allocation = _env.ScratchBufferPool.Allocate(this, 1);
            var page = _env.ScratchBufferPool.ReadPage(this, allocation.ScratchFileNumber, allocation.PositionInScratchBuffer);
            
            _transactionHeaderPage = allocation;

            UnmanagedMemory.Set(page.Base, 0, AbstractPager.PageSize);
            _txHeader = (TransactionHeader*)page.Base;
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

        private void MarkTreesForWriteTransaction()
        {
            if (_state.Root != null)
                _state.Root.InWriteTransaction = true;
            if (_state.FreeSpaceRoot != null)
                _state.FreeSpaceRoot.InWriteTransaction = true;
            foreach (var tree in Trees)
            {
                tree.State.InWriteTransaction = true;
            }
        }

        public Tree ReadTree(string treeName)
        {
            Tree tree;
            if (_trees.TryGetValue(treeName, out tree))
                return tree;

            var header = (TreeRootHeader*)Root.DirectRead((Slice)treeName);
            if (header != null)
            {
                tree = Tree.Open(this, header);
                tree.Name = treeName;
                _trees.Add(treeName, tree);
                return tree;
            }

            _trees.Add(treeName, null);
            return null;
        }

        internal Page ModifyPage(long num, Tree tree, Page page)
        {
            _env.AssertFlushingNotFailed();

            page = page ?? GetReadOnlyPage(num);
            if (page.Dirty)
                return page;

            if (_dirtyPages.Contains(num))
            {
                page.Dirty = true;
                return page;
            }

            var newPage = AllocatePage(1, PageFlags.None, num); // allocate new page in a log file but with the same number

            Memory.Copy(newPage.Base, page.Base, AbstractPager.PageSize);
            newPage.LastSearchPosition = page.LastSearchPosition;
            newPage.LastMatch = page.LastMatch;
            tree.RecentlyFoundPages.Reset(num);

            TrackWritablePage(newPage);

            return newPage;
        }

        private class PagerStateCacheItem
        {
            public readonly int FileNumber;
            public readonly PagerState State;

            public PagerStateCacheItem( int file, PagerState state )
            {
                this.FileNumber = file;
                this.State = state;
            }
        }

        private const int InvalidScratchFile = -1;
        private PagerStateCacheItem lastScratchFileUsed = new PagerStateCacheItem(InvalidScratchFile, null);
        private bool _disposed;

        public Page GetReadOnlyPage(long pageNumber)
        {
            if (_disposed)
                throw new ObjectDisposedException("Transaction");
            Page p;

            PageFromScratchBuffer value;
            if ( _scratchPagesTable.TryGetValue(pageNumber, out value))
            {
                PagerState state = null;
                if ( _scratchPagerStates != null )
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
               
                p = _env.ScratchBufferPool.ReadPage(this, value.ScratchFileNumber, value.PositionInScratchBuffer, state);
            }
            else
            {
                p = _journal.ReadPage(this, pageNumber, _scratchPagerStates) ?? _dataPager.Read(this, pageNumber);
            }

            Debug.Assert(p != null && p.PageNumber == pageNumber, string.Format("Requested ReadOnly page #{0}. Got #{1} from {2}", pageNumber, p.PageNumber, p.Source));

            TrackReadOnlyPage(p);

            return p;
        }

        internal Page AllocatePage(int numberOfPages, PageFlags flags, long? pageNumber = null)
        {
            if (_disposed)
                throw new ObjectDisposedException("Transaction");

            if (pageNumber == null)
            {
                pageNumber = _freeSpaceHandling.TryAllocateFromFreeSpace(this, numberOfPages);
                if (pageNumber == null) // allocate from end of file
                {
                    pageNumber = State.NextPageNumber;
                    State.NextPageNumber += numberOfPages;
                }
            }

            if (_env.Options.MaxStorageSize.HasValue) // check against quota
            {
                var maxAvailablePageNumber = _env.Options.MaxStorageSize / AbstractPager.PageSize;

                if(pageNumber.Value > maxAvailablePageNumber)
                    throw new QuotaException(
                        string.Format(
                            "The maximum storage size quota ({0} bytes) has been reached. " +
                            "Currently configured storage quota is allowing to allocate the following maximum page number {1}, while the requested page number is {2}. " +
                            "To increase the quota, use the MaxStorageSize property on the storage environment options.",
                            _env.Options.MaxStorageSize, maxAvailablePageNumber, pageNumber.Value));
            }


            Debug.Assert(pageNumber < State.NextPageNumber);

            var pageFromScratchBuffer = _env.ScratchBufferPool.Allocate(this, numberOfPages);
            _transactionPages.Add(pageFromScratchBuffer);

            var page = _env.ScratchBufferPool.ReadPage(this, pageFromScratchBuffer.ScratchFileNumber, pageFromScratchBuffer.PositionInScratchBuffer);
            page.PageNumber = pageNumber.Value;

            _allocatedPagesInTransaction++;
            if (numberOfPages > 1)
            {
                _overflowPagesInTransaction += (numberOfPages - 1);
            }

            _scratchPagesTable[pageNumber.Value] = pageFromScratchBuffer;

            TrackWritablePage(page);

            page.Lower = (ushort)Constants.PageHeaderSize;
            page.Flags = flags;

            if ((flags & PageFlags.KeysPrefixed) == PageFlags.KeysPrefixed)
            {
                page.Upper = (ushort) (AbstractPager.PageSize - Constants.PrefixInfoSectionSize);
                page.ClearPrefixInfo();
            }
            else
                page.Upper = AbstractPager.PageSize;

            page.Dirty = true;

            _dirtyPages.Add(page.PageNumber);

            if (numberOfPages > 1)
                _dirtyOverflowPages.Add(page.PageNumber + 1, numberOfPages - 1);

            return page;
        }

        public IEnumerable<Tree> Trees
        {
            get { return _trees.Values; }
        }

        internal int GetNumberOfFreePages(NodeHeader* node)
        {
            return GetNodeDataSize(node) / Constants.PageNumberSize;
        }

        private int GetNodeDataSize(NodeHeader* node)
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
            if (_disposed)
                throw new ObjectDisposedException("Transaction");

            if (Flags != (TransactionFlags.ReadWrite))
                return; // nothing to do

            if (Committed)
                throw new InvalidOperationException("Cannot commit already committed transaction.");

            if (RolledBack)
                throw new InvalidOperationException("Cannot commit rolled-back transaction.");

            FlushAllMultiValues();

            while (_pagesToFreeOnCommit.Count > 0)
            {
                FreePage(_pagesToFreeOnCommit.Pop());
            }

            State.Root.InWriteTransaction = false;
            State.FreeSpaceRoot.InWriteTransaction = false;

            foreach (var tree in Trees)
            {
                if (tree == null)
                    continue;
                tree.State.InWriteTransaction = false;
                var treeState = tree.State;
                if (treeState.IsModified)
                {
                    var treePtr = (TreeRootHeader*)Root.DirectAdd((Slice) tree.Name, sizeof(TreeRootHeader));
                    treeState.CopyTo(treePtr);
                }
            }

#if DEBUG
            if (State.Root != null && State.FreeSpaceRoot != null)
            {
                Debug.Assert(State.Root.RootPageNumber != State.FreeSpaceRoot.RootPageNumber);
            }
#endif

            _txHeader->LastPageNumber = _state.NextPageNumber - 1;
            _txHeader->PageCount = _allocatedPagesInTransaction;
            _txHeader->OverflowPageCount = _overflowPagesInTransaction;
            _state.Root.CopyTo(&_txHeader->Root);
            _state.FreeSpaceRoot.CopyTo(&_txHeader->FreeSpace);

            _txHeader->TxMarker |= TransactionMarker.Commit;

            if (_allocatedPagesInTransaction + _overflowPagesInTransaction > 0) // nothing changed in this transaction
            {
                _journal.WriteToJournal(this, _allocatedPagesInTransaction + _overflowPagesInTransaction + PagesTakenByHeader);
                FlushedToJournal = true;
            }

            ValidateAllPages();

            // release scratch file page allocated for the transaction header
            _env.ScratchBufferPool.Free(_transactionHeaderPage.ScratchFileNumber, _transactionHeaderPage.PositionInScratchBuffer, -1);            

            Committed = true;
            AfterCommit(this);

            if (Environment.IsDebugRecording)
            {
                RecordTransactionState(this, DebugActionType.TransactionCommit);
            }
        }


        public void Rollback()
        {
            if (_disposed)
                throw new ObjectDisposedException("Transaction");

            if (Committed || RolledBack || Flags != (TransactionFlags.ReadWrite))
                return;

            ValidateReadOnlyPages();

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
            if (Environment.IsDebugRecording)
            {
                RecordTransactionState(this, DebugActionType.TransactionRollback);
            }
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

                var trh = (TreeRootHeader*)parentTree.DirectAdd(key, sizeof(TreeRootHeader), NodeFlags.MultiValuePageRef);
                childTree.State.CopyTo(trh);

                //parentTree.SetAsMultiValueTreeRef(this, key);
            }
        }

        public bool IsDisposed
        {
            get { return _disposed; }
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            if (Environment.IsDebugRecording)
                RecordTransactionState(this, DebugActionType.TransactionDisposing);

            if (!Committed && !RolledBack && Flags == TransactionFlags.ReadWrite)
                Rollback();

            _disposed = true;

            _env.TransactionCompleted(this);
            foreach (var pagerState in _pagerStates)
            {
                pagerState.Release();
            }
        }

        internal void FreePageOnCommit(long pageNumber)
        {
            _pagesToFreeOnCommit.Push(pageNumber);
        }

        internal void FreePage(long pageNumber)
        {
            if (_disposed)
                throw new ObjectDisposedException("Transaction");

            UntrackPage(pageNumber);

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

        internal void UpdateRootsIfNeeded(Tree root, Tree freeSpace)
        {
            Debug.Assert(freeSpace.IsFreeSpaceTree);

            //can only happen during initial transaction that creates Root and FreeSpaceRoot trees
            if (State.Root != null || State.FreeSpaceRoot != null) 
                return;

            State.Root = root.State;
            State.FreeSpaceRoot = freeSpace.State;

            _root = root;
            _freeSpace = freeSpace;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void EnsurePagerStateReference(PagerState state)
        {
            if (_pagerStates.Add(state))
                state.AddRef();
        }

        internal void AddMultiValueTree(Tree tree, Slice key, Tree mvTree)
        {
            if (_multiValueTrees == null)
                _multiValueTrees = new Dictionary<Tuple<Tree, Slice>, Tree>(new TreeAndSliceComparer());
            mvTree.IsMultiValueTree = true;
            _multiValueTrees.Add(Tuple.Create(tree, key.Clone()), mvTree);
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

        internal void RemoveTree(string name)
        {
            if (_multiValueTrees != null)
            {
                var toRemove = new List<Tuple<Tree, Slice>>();

                foreach (var valueTree in _multiValueTrees)
                {
                    var multiTree = valueTree.Key.Item1;

                    if (multiTree.Name == name)
                    {
                        toRemove.Add(valueTree.Key);
                    }
                }

                foreach (var recordToRemove in toRemove)
                {
                    _multiValueTrees.Remove(recordToRemove);
                }
            }

            _trees.Remove(name);
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

        internal void AddTree(string name, Tree tree)
        {
            Tree value;
            if (_trees.TryGetValue(name, out value) && value != null)
            {
                throw new InvalidOperationException("Tree already exists: " + name);
            }
            _trees[name] = tree;
        }

        internal Transaction JournalApplicatorTransaction()
        {
            CreatedByJournalApplicator = true;
            return this;
        }

#if VALIDATE_PAGES

        private Dictionary<long, ulong> readOnlyPages = new Dictionary<long, ulong>();
        private Dictionary<long, ulong> writablePages = new Dictionary<long, ulong>();

        private void ValidateAllPages()
        {
            ValidateWritablePages();
            ValidateReadOnlyPages();
        }

        private void ValidateReadOnlyPages()
        {
            foreach(var readOnlyKey in readOnlyPages )
            {
                long pageNumber = readOnlyKey.Key;
                if (_dirtyPages.Contains(pageNumber))
                    throw new VoronUnrecoverableErrorException("Read only page is dirty (which means you are modifying a page directly in the data -- non transactionally -- ).");

                var page = this.GetReadOnlyPage(pageNumber);

                ulong pageHash = Hashing.XXHash64.Calculate(page.Base, page.PageSize);
                if (pageHash != readOnlyKey.Value)
                    throw new VoronUnrecoverableErrorException("Read only page content is different (which means you are modifying a page directly in the data -- non transactionally -- ).");
            }
        }

        private void ValidateWritablePages()
        {
            foreach(var writableKey in writablePages)
            {
                long pageNumber = writableKey.Key;
                if (!_dirtyPages.Contains(pageNumber))
                    throw new VoronUnrecoverableErrorException("Writable key is not dirty (which means you are asking for a page modification for no reason).");
            }
        }

        private void UntrackPage(long pageNumber)
        {
            readOnlyPages.Remove(pageNumber);
            writablePages.Remove(pageNumber);                
        }

        private void TrackWritablePage(Page page)
        {
            if (readOnlyPages.ContainsKey(page.PageNumber))
                readOnlyPages.Remove(page.PageNumber);            

            if (!writablePages.ContainsKey(page.PageNumber))
            {
                ulong pageHash = Hashing.XXHash64.Calculate(page.Base, page.PageSize);
                writablePages[page.PageNumber] = pageHash;
            }
        }

        private void TrackReadOnlyPage(Page page)
        {
            if (writablePages.ContainsKey(page.PageNumber))
                return;

            ulong pageHash = Hashing.XXHash64.Calculate(page.Base, page.PageSize);

            ulong storedHash;
            if ( readOnlyPages.TryGetValue(page.PageNumber, out storedHash) )
            {
                if (pageHash != storedHash)
                    throw new VoronUnrecoverableErrorException("Read Only Page has change between tracking requests. Page #" + page.PageNumber);
            }
            else
            {
                readOnlyPages[page.PageNumber] = pageHash;
            }
        }

#else
        // This will only be used as placeholder for compilation when not running with validation started.

        [Conditional("VALIDATE_PAGES")]
        private void ValidateAllPages() { }

        [Conditional("VALIDATE_PAGES")]
        private void ValidateReadOnlyPages() { }

        [Conditional("VALIDATE_PAGES")]
        private void TrackWritablePage(Page page) { }

        [Conditional("VALIDATE_PAGES")]
        private void TrackReadOnlyPage(Page page) { }

        [Conditional("VALIDATE_PAGES")]
        private void UntrackPage(long pageNumber) { }
#endif

    }
}
