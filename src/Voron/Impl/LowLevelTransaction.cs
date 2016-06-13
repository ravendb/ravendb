using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Sparrow;
using Sparrow.Platform;
using Voron.Data.BTrees;
using Voron.Exceptions;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Voron.Data;
using System.Runtime.InteropServices;

namespace Voron.Impl
{
    public unsafe class LowLevelTransaction : IDisposable
    {
        private const int PagesTakenByHeader = 1;
        private readonly IVirtualPager _dataPager;
        private readonly long _pageSize;
        private readonly StorageEnvironment _env;
        private readonly long _id;
        private readonly ByteStringContext _allocator;
        private readonly PageCache _pageCache;
        private Tree _root;

        public bool FlushedToJournal { get; private set; }
        public Tree RootObjects => _root;

        private readonly WriteAheadJournal _journal;

        private readonly HashSet<long> _dirtyPages = new HashSet<long>(NumericEqualityComparer.Instance);
        private readonly Dictionary<long, long> _dirtyOverflowPages = new Dictionary<long, long>(NumericEqualityComparer.Instance);
        private readonly HashSet<PagerState> _pagerStates = new HashSet<PagerState>();

        readonly Stack<long> _pagesToFreeOnCommit = new Stack<long>();

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

        public TransactionFlags Flags { get; }
        public bool IsLazyTransaction { get; set; }


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

        public readonly int PageSize;

        public bool Committed { get; private set; }

        public bool RolledBack { get; private set; }

        public StorageEnvironmentState State
        {
            get { return _state; }
        }

        public ByteStringContext Allocator
        {
            get { return _allocator; }
        }

        public ulong Hash
        {
            get { return _txHeader->Hash; }
        }

        public LowLevelTransaction(StorageEnvironment env, long id, TransactionFlags flags, IFreeSpaceHandling freeSpaceHandling, ByteStringContext context = null )
        {
            _dataPager = env.Options.DataPager;            
            _pageSize = this.DataPager.PageSize;
            _env = env;
            _journal = env.Journal;
            _id = id;
            _freeSpaceHandling = freeSpaceHandling;
            _allocator = context ?? new ByteStringContext();
            _pageCache = new PageCache(this, 8);

            Flags = flags;
            PageSize = _dataPager.PageSize;

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
            bool isReadOnly;
            Page currentPage = _pageCache.TryGetWritablePage(num, out isReadOnly);
            if (currentPage != null && !isReadOnly)
                return currentPage;

            // We only go to get it if we had a complete cache miss (not even read). 
            if ( currentPage == null )
                currentPage = GetPage(num);

            if (_dirtyPages.Contains(num))
                return currentPage;

            // We no longer care of the ReadOnly cached page (in case the get page was a hit).
            _pageCache.Reset(num);

            int pageSize;
            Page newPage;
            if ( currentPage.IsOverflow )
            {
                newPage = AllocateOverflowRawPage(currentPage.OverflowSize, num);
                pageSize = currentPage.OverflowSize;
            }
            else
            {
                newPage = AllocatePage(1, num); // allocate new page in a log file but with the same number			
                pageSize = Environment.Options.PageSize;
            }
            
            Memory.BulkCopy(newPage.Pointer, currentPage.Pointer, pageSize);

            TrackWritablePage(newPage);
            
            // We add the new page. 
            _pageCache.AddWritable(newPage);

            return newPage;
        }

        private const int InvalidScratchFile = -1;
        private PagerStateCacheItem lastScratchFileUsed = new PagerStateCacheItem(InvalidScratchFile, null);
        private bool _disposed;

        public Page GetPage(long pageNumber)
        {	        
            if (_disposed)
                throw new ObjectDisposedException("Transaction");

            // Check if we can hit the lowest level locality cache.
            Page p = _pageCache.TryGetReadOnlyPage(pageNumber);
            if (p != null)
                return p; 

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

                p = _env.ScratchBufferPool.ReadPage(this, value.ScratchFileNumber, value.PositionInScratchBuffer, state);
                Debug.Assert(p != null && p.PageNumber == pageNumber, string.Format("Requested ReadOnly page #{0}. Got #{1} from {2}", pageNumber, p.PageNumber, p.Source));
            }
            else
            {
                p = _journal.ReadPage(this, pageNumber, _scratchPagerStates) ?? _dataPager.ReadPage(this, pageNumber);
                Debug.Assert(p != null && p.PageNumber == pageNumber, string.Format("Requested ReadOnly page #{0}. Got #{1} from {2}", pageNumber, p.PageNumber, p.Source));
            }
            
            TrackReadOnlyPage(p);

            _pageCache.AddReadOnly(p);
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

        private Page AllocateOverflowPage(long headerSize, long dataSize, long? pageNumber = null)
        {
            long overflowSize = headerSize + dataSize;
            if (overflowSize > int.MaxValue - 1)
                throw new InvalidOperationException($"Cannot allocate chunks bigger than { int.MaxValue / 1024 * 1024 } Mb.");

            Debug.Assert(overflowSize >= 0);

            long numberOfPages = (overflowSize / _pageSize) + (overflowSize % _pageSize == 0 ? 0 : 1);

            var overflowPage = AllocatePage((int)numberOfPages, pageNumber);
            overflowPage.Flags = PageFlags.Overflow;
            overflowPage.OverflowSize = (int)overflowSize;

            return overflowPage;
        }

        public Page AllocateOverflowPage(long dataSize, long? pageNumber = null) 
        {
            return AllocateOverflowPage(sizeof(PageHeader), dataSize, pageNumber);
        }

        public Page AllocateOverflowPage<T>(long dataSize, long? pageNumber = null) where T : struct
        {
            return AllocateOverflowPage(Marshal.SizeOf<T>(), dataSize, pageNumber);
        }

        public Page AllocateOverflowRawPage(long pageSize, long? pageNumber = null)
        {
            return AllocateOverflowPage(0, pageSize, pageNumber);
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

#if VALIDATE
            VerifyNoDuplicateScratchPages();
#endif
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

            var newPage = _env.ScratchBufferPool.ReadPage(this, pageFromScratchBuffer.ScratchFileNumber,
                pageFromScratchBuffer.PositionInScratchBuffer);
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

            _pageCache.Reset(pageNumber);

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
            _txHeader->PageCount = _allocatedPagesInTransaction;
            _txHeader->OverflowPageCount = _overflowPagesInTransaction;
            _state.Root.CopyTo(&_txHeader->Root);

            _txHeader->TxMarker |= TransactionMarker.Commit;

            if (IsLazyTransaction && Environment.IsFlushingScratchBuffer)
                IsLazyTransaction = false;

            if (_allocatedPagesInTransaction + _overflowPagesInTransaction > 0 || // nothing changed in this transaction
                (IsLazyTransaction == false && _journal != null && _journal.HasDataInLazyTxBuffer()))  // allow call to writeToJournal for flushing lazy tx
            {
                _journal.WriteToJournal(this, _allocatedPagesInTransaction + _overflowPagesInTransaction + PagesTakenByHeader);
                FlushedToJournal = true;
            }

            ValidateAllPages();

            // release scratch file page allocated for the transaction header
            _env.ScratchBufferPool.Free(_transactionHeaderPage.ScratchFileNumber, _transactionHeaderPage.PositionInScratchBuffer, -1);

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

            RolledBack = true;
        }


        internal LowLevelTransaction JournalApplicatorTransaction()
        {
            CreatedByJournalApplicator = true;
            return this;
        }

        private PagerState _lastState;

        internal void EnsurePagerStateReference(PagerState state)
        {
            if (state == _lastState || state == null)
                return;

            if (_pagerStates.Add(state))
            {
                state.AddRef();
                _lastState = state;
            }
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

                ulong pageHash = Hashing.XXHash64.Calculate(page.Pointer, Environment.Options.PageSize);
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
                ulong pageHash = Hashing.XXHash64.Calculate(page.Pointer, Environment.Options.PageSize);
                writablePages[page.PageNumber] = pageHash;
            }
        }

        private void TrackReadOnlyPage(Page page)
        {
            if (writablePages.ContainsKey(page.PageNumber))
                return;

            ulong pageHash = Hashing.XXHash64.Calculate(page.Pointer, Environment.Options.PageSize);

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


        private class PageCache
        {
            private readonly LowLevelTransaction _tx;
            private readonly PageHandlePtr[] _cache;
            private int current = 0;

            public PageCache(LowLevelTransaction tx, int cacheSize = 8)
            {
                Debug.Assert(tx != null);

                this._tx = tx;
                this._cache = new PageHandlePtr[cacheSize];
            }

            public Page TryGetReadOnlyPage(long pageNumber)
            {
                int position = current;

                int itemsLeft = _cache.Length;
                while (itemsLeft > 0)
                {
                    int i = position % _cache.Length;

                    // If the value is not valid or the page number is not equal
                    if (!_cache[i].IsValid || _cache[i].PageNumber != pageNumber)
                    {
                        // we continue.
                        itemsLeft--;
                        position++;

                        continue;
                    }

                    return _cache[i].Value;
                }

                return null;
            }

            private const int Invalid = -1;

            public Page TryGetWritablePage(long pageNumber, out bool isReadOnly)
            {
                isReadOnly = false;
                int position = current;

                int itemsLeft = _cache.Length;
                while (itemsLeft > 0)
                {
                    int i = position % _cache.Length;

                    // If the value is not valid or the page number is not equal
                    if (!_cache[i].IsValid || _cache[i].PageNumber != pageNumber)
                    {
                        // we continue.
                        itemsLeft--;
                        position++;

                        continue;
                    }

                    if (!_cache[i].IsWritable)
                    {
                        Page result = _cache[i].Value;
                        isReadOnly = true;

                        _cache[i] = default(PageHandlePtr);
                        return result;
                    }
                                               
                    return _cache[i].Value;
                }

                return null;
            }

            public void AddWritable(Page page)
            {
                current = (++current) % _cache.Length;
                _cache[current] = new PageHandlePtr(page, true);
            }

            public void AddReadOnly(Page page)
            {
                current = (++current) % _cache.Length;
                _cache[current] = new PageHandlePtr(page, false);
            }

            public void Clear()
            {
                Array.Clear(_cache, 0, _cache.Length);
            }

            public void Reset(long pageNumber)
            {
                for (int i = 0; i < _cache.Length; i++)
                {
                    if (_cache[i].IsValid && _cache[i].PageNumber == pageNumber)
                    {
                        _cache[i] = new PageHandlePtr();
                        return;
                    }
                }
            }
        }
    }
}