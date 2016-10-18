using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Sparrow;
using Voron.Data.BTrees;
using Voron.Exceptions;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Voron.Global;
using Voron.Debugging;
using Voron.Util;

namespace Voron.Impl
{
    public unsafe class LowLevelTransaction : IDisposable
    {
        private const int PagesTakenByHeader = 1;
        public readonly AbstractPager DataPager;
        private readonly StorageEnvironment _env;
        private readonly long _id;
        private readonly ByteStringContext _allocator;
        private readonly bool _disposeAllocator;
        private Tree _root;

        public bool FlushedToJournal;
        public Tree RootObjects => _root;

        private readonly WriteAheadJournal _journal;

        private readonly HashSet<long> _dirtyPages;
        private readonly Dictionary<long, long> _dirtyOverflowPages;

        readonly Stack<long> _pagesToFreeOnCommit;

        private readonly IFreeSpaceHandling _freeSpaceHandling;

        private int _allocatedPagesInTransaction;
        private int _overflowPagesInTransaction;
        private TransactionHeader* _txHeader;

        private PageFromScratchBuffer _transactionHeaderPage;
        private readonly HashSet<PageFromScratchBuffer> _transactionPages;
        private readonly HashSet<long> _freedPages;
        private readonly List<PageFromScratchBuffer> _unusedScratchPages;

        private readonly Dictionary<long, PageFromScratchBuffer> _scratchPagesTable;

        private readonly List<PagerState> _pagerStates = new List<PagerState>(4);
        internal readonly List<JournalSnapshot> JournalSnapshots = new List<JournalSnapshot>();

        private readonly StorageEnvironmentState _state;
        private readonly Dictionary<int, PagerState> _scratchPagerStates;
        private CommitStats _requestedCommitStats;

        public TransactionFlags Flags { get; }

        public bool IsLazyTransaction
        {
            get { return _isLazyTransaction; }
            set
            {
                _isLazyTransaction = value;
                if (_isLazyTransaction)
                    _env.Journal.HasLazyTransactions = true;
            }
        }


        internal bool CreatedByJournalApplicator;

        internal StorageEnvironment Environment => _env;

        public long Id => _id;

        public readonly int PageSize;

        public bool Committed { get; private set; }

        public bool RolledBack { get; private set; }

        public StorageEnvironmentState State => _state;

        public ByteStringContext Allocator => _allocator;

        public ulong Hash => _txHeader->Hash;

        public LowLevelTransaction(StorageEnvironment env, long id, TransactionFlags flags, IFreeSpaceHandling freeSpaceHandling, ByteStringContext context = null )
        {
            DataPager = env.Options.DataPager;            
            _env = env;
            _journal = env.Journal;
            _id = id;
            _freeSpaceHandling = freeSpaceHandling;
            _allocator = context ?? new ByteStringContext();
            _disposeAllocator = context == null;

            Flags = flags;
         
            PageSize = DataPager.PageSize;

            var scratchPagerStates = env.ScratchBufferPool.GetPagerStatesOfAllScratches();
            foreach (var scratchPagerState in scratchPagerStates.Values)
            {
                scratchPagerState.AddRef();
                _pagerStates.Add(scratchPagerState);
            }

            if (flags != TransactionFlags.ReadWrite)
            {
                // for read transactions, we need to keep the pager state frozen
                // for write transactions, we can use the current one (which == null)
                _scratchPagerStates = scratchPagerStates;

                _state = env.State.Clone();

                InitializeRoots();

                JournalSnapshots = _journal.GetSnapshots();
               

                return;
            }

            _dirtyOverflowPages = new Dictionary<long, long>(NumericEqualityComparer.Instance);
            _scratchPagesTable = new Dictionary<long, PageFromScratchBuffer>(NumericEqualityComparer.Instance);
            _dirtyPages = new HashSet<long>(NumericEqualityComparer.Instance);
            _freedPages = new HashSet<long>();
            _unusedScratchPages = new List<PageFromScratchBuffer>();
            _transactionPages = new HashSet<PageFromScratchBuffer>();
            _pagesToFreeOnCommit = new Stack<long>();

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
                _root = new Tree(this, null, _state.Root) { Name = Constants.RootTreeNameSlice };
            }
        }

        private void InitTransactionHeader()
        {
            var allocation = _env.ScratchBufferPool.Allocate(this, 1);
            var page = _env.ScratchBufferPool.ReadPage(this, allocation.ScratchFileNumber, allocation.PositionInScratchBuffer);

            _transactionHeaderPage = allocation;

            UnmanagedMemory.Set(page.Pointer, 0, Environment.Options.PageSize);
            _txHeader = (TransactionHeader*)page.Pointer;
            _txHeader->HeaderMarker = Constants.TransactionHeaderMarker;

            _txHeader->TransactionId = _id;
            _txHeader->NextPageNumber = _state.NextPageNumber;
            _txHeader->LastPageNumber = -1;
            _txHeader->PageCount = -1;
            _txHeader->Hash = 0;
            _txHeader->TimeStampTicksUtc = DateTime.UtcNow.Ticks;
            _txHeader->TxMarker = TransactionMarker.None;
            _txHeader->CompressedSize = 0;
            _txHeader->UncompressedSize = 0;

            _allocatedPagesInTransaction = 0;
            _overflowPagesInTransaction = 0;

            _scratchPagesTable.Clear();
        }

        internal PageFromScratchBuffer GetTransactionHeaderPage()
        {
            return this._transactionHeaderPage;
        }

        internal HashSet<PageFromScratchBuffer> GetTransactionPages()
        {
            VerifyNoDuplicateScratchPages();
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

            // Check if we can hit the lowest level locality cache.
            Page currentPage = GetPage(num);

            if (_dirtyPages.Contains(num))
                return currentPage;

            int pageSize;
            Page newPage;
            if ( currentPage.IsOverflow )
            {
                newPage = AllocateOverflowRawPage(currentPage.OverflowSize, num, currentPage);
                pageSize = Environment.Options.PageSize*
                           DataPager.GetNumberOfOverflowPages(currentPage.OverflowSize);
            }
            else
            {
                newPage = AllocatePage(1, num, currentPage); // allocate new page in a log file but with the same number			
                pageSize = Environment.Options.PageSize;
            }

            Memory.BulkCopy(newPage.Pointer, currentPage.Pointer, pageSize);

            TrackWritablePage(newPage);           

            return newPage;
        }

        private const int InvalidScratchFile = -1;
        private PagerStateCacheItem _lastScratchFileUsed = new PagerStateCacheItem(InvalidScratchFile, null);
        private bool _disposed;

        public Page GetPage(long pageNumber)
        {	        
            if (_disposed)
                throw new ObjectDisposedException("Transaction");

            // Check if we can hit the lowest level locality cache.
            Page p;
            PageFromScratchBuffer value;
            if (_scratchPagesTable != null && _scratchPagesTable.TryGetValue(pageNumber, out value) )
            {
                PagerState state = null;
                if (_scratchPagerStates != null)
                {
                    var lastUsed = _lastScratchFileUsed;
                    if (lastUsed.FileNumber == value.ScratchFileNumber)
                    {
                        state = lastUsed.State;
                    }
                    else
                    {
                        state = _scratchPagerStates[value.ScratchFileNumber];
                        _lastScratchFileUsed = new PagerStateCacheItem(value.ScratchFileNumber, state);
                    }
                }

                p = _env.ScratchBufferPool.ReadPage(this, value.ScratchFileNumber, value.PositionInScratchBuffer, state);
                Debug.Assert(p != null && p.PageNumber == pageNumber, string.Format("Requested ReadOnly page #{0}. Got #{1} from {2}", pageNumber, p.PageNumber, p.Source));
            }
            else
            {
                p = _journal.ReadPage(this, pageNumber, _scratchPagerStates) ?? DataPager.ReadPage(this, pageNumber);
                Debug.Assert(p != null && p.PageNumber == pageNumber, string.Format("Requested ReadOnly page #{0}. Got #{1} from {2}", pageNumber, p.PageNumber, p.Source));
            }
            
            TrackReadOnlyPage(p);

            return p;
        }

        public Page AllocatePage(int numberOfPages, long? pageNumber = null, Page previousPage = null)
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
            return AllocatePage(numberOfPages, pageNumber.Value, previousPage);
        }

        public Page AllocateOverflowRawPage(long pageSize, long? pageNumber = null, Page previousPage = null)
        {
            long overflowSize = 0 + pageSize;
            if (overflowSize > int.MaxValue - 1)
                throw new InvalidOperationException($"Cannot allocate chunks bigger than { int.MaxValue / 1024 * 1024 } Mb.");

            Debug.Assert(overflowSize >= 0);

            long numberOfPages = (overflowSize / PageSize) + (overflowSize % PageSize == 0 ? 0 : 1);

            var overflowPage = AllocatePage((int)numberOfPages, pageNumber, previousPage);
            overflowPage.Flags = PageFlags.Overflow;
            overflowPage.OverflowSize = (int)overflowSize;

            return overflowPage;
        }

        private Page AllocatePage(int numberOfPages, long pageNumber, Page previousVersion)
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

#if VALIDATE
            VerifyNoDuplicateScratchPages();
#endif
            var pageFromScratchBuffer = _env.ScratchBufferPool.Allocate(this, numberOfPages);
            pageFromScratchBuffer.PreviousVersion = previousVersion;
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

            var newPage = _env.ScratchBufferPool.ReadPage(this, pageFromScratchBuffer.ScratchFileNumber,
                pageFromScratchBuffer.PositionInScratchBuffer);
            
            UnmanagedMemory.Set(newPage.Pointer, 0, Environment.Options.PageSize * numberOfPages);
            newPage.PageNumber = pageNumber;
            newPage.Flags = PageFlags.Single;

            TrackWritablePage(newPage);

#if VALIDATE
            VerifyNoDuplicateScratchPages();
#endif

            return newPage;
        }

        internal void BreakLargeAllocationToSeparatePages(long pageNumber)
        {
            if (_disposed)
                throw new ObjectDisposedException("Transaction");

            PageFromScratchBuffer value;
            if (_scratchPagesTable.TryGetValue(pageNumber, out value) == false)
                throw new InvalidOperationException("The page " + pageNumber + " was not previous allocated in this transaction");

            if (value.NumberOfPages == 1)
                return;

            _transactionPages.Remove(value);
            _env.ScratchBufferPool.BreakLargeAllocationToSeparatePages(value);
            _allocatedPagesInTransaction += value.NumberOfPages - 1;
            _overflowPagesInTransaction -= value.NumberOfPages - 1;

            for (int i = 0; i < value.NumberOfPages; i++)
            {
                var pageFromScratchBuffer = new PageFromScratchBuffer(value.ScratchFileNumber, value.PositionInScratchBuffer + i, 1, 1);
                _transactionPages.Add(pageFromScratchBuffer);
                _scratchPagesTable[pageNumber + i] = pageFromScratchBuffer;
                _dirtyOverflowPages.Remove(pageNumber + i);
                _dirtyPages.Add(pageNumber + i);
                var newPage = _env.ScratchBufferPool.ReadPage(this, value.ScratchFileNumber, value.PositionInScratchBuffer + i);
                newPage.PageNumber = pageNumber + i;
                newPage.Flags = PageFlags.Single;
                TrackWritablePage(newPage);
            }
        }


        [Conditional("DEBUG")]
        public void VerifyNoDuplicateScratchPages()
        {
            var pageNums = new HashSet<long>();
            foreach (var txPage in _transactionPages)
            {
                var scratchPage = Environment.ScratchBufferPool.ReadPage(this, txPage.ScratchFileNumber,
                    txPage.PositionInScratchBuffer);
                if (pageNums.Add(scratchPage.PageNumber) == false)
                    throw new InvalidDataException("Duplicate page in transaction: " + scratchPage.PageNumber);
            }
        }


        public bool IsDisposed => _disposed;

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

            if (_disposeAllocator)
                _allocator.Dispose();
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

            while (_pagesToFreeOnCommit.Count > 0)
            {
                FreePage(_pagesToFreeOnCommit.Pop());
            }
            _txHeader->LastPageNumber = _state.NextPageNumber - 1;
            _state.Root.CopyTo(&_txHeader->Root);

            _txHeader->TxMarker |= TransactionMarker.Commit;

            if (IsLazyTransaction && Environment.IsFlushingScratchBuffer)
                IsLazyTransaction = false;

            var totalNumberOfAllocatedPages = _allocatedPagesInTransaction + _overflowPagesInTransaction;
            if (totalNumberOfAllocatedPages > 0 || // nothing changed in this transaction
                (this.IsLazyTransaction == false && this._journal != null && this._journal.HasDataInLazyTxBuffer()))  // allow call to writeToJournal for flushing lazy tx
            {
                var numberOfWrittenPages = _journal.WriteToJournal(this, totalNumberOfAllocatedPages + PagesTakenByHeader);
                FlushedToJournal = true;

                if (_requestedCommitStats != null)
                {
                    _requestedCommitStats.NumberOfModifiedPages = totalNumberOfAllocatedPages + PagesTakenByHeader;
                    _requestedCommitStats.NumberOfPagesWrittenToDisk = numberOfWrittenPages;
                }
            }

            ValidateAllPages();

            // release scratch file page allocated for the transaction header
            _env.ScratchBufferPool.Free(_transactionHeaderPage.ScratchFileNumber, _transactionHeaderPage.PositionInScratchBuffer, -1);

            _env.ScratchBufferPool.UpdateCacheForPagerStatesOfAllScratches();
            _env.Journal.UpdateCacheForJournalSnapshots();

            Committed = true;
            _env.TransactionAfterCommit(this);
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

            _env.ScratchBufferPool.UpdateCacheForPagerStatesOfAllScratches();
            _env.Journal.UpdateCacheForJournalSnapshots();

            RolledBack = true;
        }
        public void RetrieveCommitStats(out CommitStats stats)
        {
            _requestedCommitStats = stats = new CommitStats();
        }


        internal LowLevelTransaction JournalApplicatorTransaction()
        {
            CreatedByJournalApplicator = true;
            return this;
        }

        private PagerState _lastState;
        private bool _isLazyTransaction;

        internal ActiveTransactions.Node ActiveTransactionNode;

        internal void EnsurePagerStateReference(PagerState state)
        {
            if (state == _lastState || state == null)
                return;

            _pagerStates.Add(state);
            state.AddRef();
            _lastState = state;
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

                var page = this.GetPage(pageNumber);

                ulong pageHash = Hashing.XXHash64.Calculate(page.Pointer, (ulong)Environment.Options.PageSize);
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
                ulong pageHash = Hashing.XXHash64.Calculate(page.Pointer, (ulong)Environment.Options.PageSize);
                writablePages[page.PageNumber] = pageHash;
            }
        }

        private void TrackReadOnlyPage(Page page)
        {
            if (writablePages.ContainsKey(page.PageNumber))
                return;

            ulong pageHash = Hashing.XXHash64.Calculate(page.Pointer, (ulong)Environment.Options.PageSize);

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