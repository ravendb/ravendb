using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Sparrow;
using Sparrow.Collections;
using Sparrow.LowMemory;
using Sparrow.Utils;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
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
    public unsafe class LowLevelTransaction : IPagerLevelTransactionState
    {
        public readonly AbstractPager DataPager;
        private readonly StorageEnvironment _env;
        private readonly long _id;
        private readonly ByteStringContext _allocator;
        private readonly PageLocator _pageLocator;
        private bool _disposeAllocator;

        private Tree _root;
        public Tree RootObjects => _root;

        public bool FlushedToJournal;

        private long _numberOfModifiedPages;

        public long NumberOfModifiedPages => _numberOfModifiedPages;

        private readonly WriteAheadJournal _journal;
        internal readonly List<JournalSnapshot> JournalSnapshots = new List<JournalSnapshot>();

        bool IPagerLevelTransactionState.IsWriteTransaction => Flags == TransactionFlags.ReadWrite;

        Dictionary<AbstractPager, TransactionState> IPagerLevelTransactionState.PagerTransactionState32Bits
        {
            get;
            set;
        }

        Dictionary<AbstractPager, CryptoTransactionState> IPagerLevelTransactionState.CryptoPagerTransactionState { get; set; }

        internal class WriteTransactionPool
        {
#if DEBUG
            public int BuilderUsages;
#endif
            public TableValueBuilder TableValueBuilder = new TableValueBuilder();

            public int ScratchPagesTablePoolIndex = 0;
            public FastDictionary<long, PageFromScratchBuffer, NumericEqualityStructComparer> ScratchPagesTablePool1 = new FastDictionary<long, PageFromScratchBuffer, NumericEqualityStructComparer>(new NumericEqualityStructComparer());
            public FastDictionary<long, PageFromScratchBuffer, NumericEqualityStructComparer> ScratchPagesTablePool2 = new FastDictionary<long, PageFromScratchBuffer, NumericEqualityStructComparer>(new NumericEqualityStructComparer());
            public FastDictionary<long, long, NumericEqualityStructComparer> DirtyOverflowPagesPool = new FastDictionary<long, long, NumericEqualityStructComparer>(new NumericEqualityStructComparer());
            public HashSet<long> DirtyPagesPool = new HashSet<long>(NumericEqualityComparer.Instance);

            public void Reset()
            {
                ScratchPagesTablePool2.Clear();
                ScratchPagesTablePool1.Clear();
                DirtyOverflowPagesPool.Clear();
                DirtyPagesPool.Clear();
                TableValueBuilder.Reset();
            }
        }

        // BEGIN: Structures that are safe to pool.
        private readonly HashSet<long> _dirtyPages;
        private readonly FastDictionary<long, long, NumericEqualityStructComparer> _dirtyOverflowPages;
        private readonly Stack<long> _pagesToFreeOnCommit;
        private readonly FastDictionary<long, PageFromScratchBuffer, NumericEqualityStructComparer> _scratchPagesTable;
        private readonly HashSet<PagerState> _pagerStates;
        private readonly Dictionary<int, PagerState> _scratchPagerStates;
        // END: Structures that are safe to pool.


        public event Action<IPagerLevelTransactionState> BeforeCommitFinalization;
        public event Action<IPagerLevelTransactionState> OnDispose;
        public event Action AfterCommitWhenNewReadTransactionsPrevented;

        private readonly IFreeSpaceHandling _freeSpaceHandling;
        internal FixedSizeTree _freeSpaceTree;

        private TransactionHeader* _txHeader;

        private readonly HashSet<PageFromScratchBuffer> _transactionPages;
        private readonly HashSet<long> _freedPages;
        private readonly List<PageFromScratchBuffer> _unusedScratchPages;


        private readonly StorageEnvironmentState _state;

        private CommitStats _requestedCommitStats;

        public TransactionPersistentContext PersistentContext { get; }
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

        public StorageEnvironment Environment => _env;

        public long Id => _id;

        public bool Committed { get; private set; }

        public bool RolledBack { get; private set; }

        public StorageEnvironmentState State => _state;

        public ByteStringContext Allocator => _allocator;

        public ulong Hash => _txHeader->Hash;

        private LowLevelTransaction(LowLevelTransaction previous)
        {
            // this is meant to be used with transaction merging only
            // so it makes a lot of assumptions about the usage scenario
            // and what it can do

            Debug.Assert(previous.Flags == TransactionFlags.ReadWrite);

            var env = previous._env;
            env.Options.AssertNoCatastrophicFailure();

            FlushInProgressLockTaken = previous.FlushInProgressLockTaken;
            CurrentTransactionHolder = previous.CurrentTransactionHolder;
            TxStartTime = DateTime.UtcNow;
            DataPager = env.Options.DataPager;
            _env = env;
            _journal = env.Journal;
            _id = previous.Id + 1;
            _freeSpaceHandling = previous._freeSpaceHandling;
            _allocator = previous._allocator;

            _disposeAllocator = previous._disposeAllocator;
            previous._disposeAllocator = false;

            PersistentContext = previous.PersistentContext;
            Flags = TransactionFlags.ReadWrite;

            _pagerStates = new HashSet<PagerState>(ReferenceEqualityComparer<PagerState>.Default);

            JournalFiles = previous.JournalFiles;

            foreach (var journalFile in JournalFiles)
            {
                journalFile.AddRef();
            }

            foreach (var scratchPagerState in previous._pagerStates)
            {
                scratchPagerState.AddRef();
                _pagerStates.Add(scratchPagerState);
            }

            EnsureNoDuplicateTransactionId(_id);

            // we can reuse those instances, not calling Reset on the pool
            // because we are going to need to scratch buffer pool
            _dirtyOverflowPages = previous._dirtyOverflowPages;
            _dirtyOverflowPages.Clear();

            _scratchPagesTable = _env.WriteTransactionPool.ScratchPagesTablePool2;

            foreach (var kvp in previous._scratchPagesTable)
            {
                if (previous._dirtyPages.Contains(kvp.Key))
                    _scratchPagesTable.Add(kvp.Key, kvp.Value);
            }
            previous._scratchPagesTable.Clear();
            _env.WriteTransactionPool.ScratchPagesTablePool1 = _scratchPagesTable;
            _env.WriteTransactionPool.ScratchPagesTablePool2 = previous._scratchPagesTable;

            _dirtyPages = previous._dirtyPages;
            _dirtyPages.Clear();

            _freedPages = new HashSet<long>(NumericEqualityComparer.Instance);
            _unusedScratchPages = new List<PageFromScratchBuffer>();
            _transactionPages = new HashSet<PageFromScratchBuffer>(PageFromScratchBufferEqualityComparer.Instance);
            _pagesToFreeOnCommit = new Stack<long>();

            _state = previous._state.Clone();

            _pageLocator = PersistentContext.AllocatePageLocator(this);
            InitializeRoots();
            InitTransactionHeader();
        }

        public LowLevelTransaction(StorageEnvironment env, long id, TransactionPersistentContext transactionPersistentContext, TransactionFlags flags, IFreeSpaceHandling freeSpaceHandling, ByteStringContext context = null)
        {
            TxStartTime = DateTime.UtcNow;
            env.Options.AssertNoCatastrophicFailure();

            DataPager = env.Options.DataPager;
            _env = env;
            _journal = env.Journal;
            _id = id;
            _freeSpaceHandling = freeSpaceHandling;
            _allocator = context ?? new ByteStringContext(LowMemoryFlag.None);
            _disposeAllocator = context == null;
            _pagerStates = new HashSet<PagerState>(ReferenceEqualityComparer<PagerState>.Default);           
            
            PersistentContext = transactionPersistentContext;
            Flags = flags;

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

                _pageLocator = transactionPersistentContext.AllocatePageLocator(this);
                InitializeRoots();

                JournalSnapshots = _journal.GetSnapshots();

                return;
            }

            EnsureNoDuplicateTransactionId(id);
            // we keep this copy to make sure that if we use async commit, we have a stable copy of the jounrals
            // as they were at the time we started the original transaction, this is required because async commit
            // may modify the list of files we have available
            JournalFiles = _journal.Files;
            foreach (var journalFile in JournalFiles)
            {
                journalFile.AddRef();
            }
            _env.WriteTransactionPool.Reset();
            _dirtyOverflowPages = _env.WriteTransactionPool.DirtyOverflowPagesPool;
            _scratchPagesTable = _env.WriteTransactionPool.ScratchPagesTablePool1;
            _dirtyPages = _env.WriteTransactionPool.DirtyPagesPool;
            _freedPages = new HashSet<long>(NumericEqualityComparer.Instance);
            _unusedScratchPages = new List<PageFromScratchBuffer>();
            _transactionPages = new HashSet<PageFromScratchBuffer>(PageFromScratchBufferEqualityComparer.Instance);
            _pagesToFreeOnCommit = new Stack<long>();

            _state = env.State.Clone();

            _pageLocator = transactionPersistentContext.AllocatePageLocator(this);
            InitializeRoots();
            InitTransactionHeader();
        }

        [Conditional("DEBUG")]
        private void EnsureNoDuplicateTransactionId(long id)
        {
            foreach (var journalFile in _journal.Files)
            {
                var lastSeenTxIdByJournal = journalFile.PageTranslationTable.GetLastSeenTransactionId();

                if (id <= lastSeenTxIdByJournal)
                    VoronUnrecoverableErrorException.Raise(_env,
                        $"PTT of journal {journalFile.Number} already contains records for a new write tx. " +
                        $"Tx id = {id}, last seen by journal = {lastSeenTxIdByJournal}");

                if (journalFile.PageTranslationTable.IsEmpty)
                    continue;

                var maxTxIdInJournal = journalFile.PageTranslationTable.MaxTransactionId();

                if (id <= maxTxIdInJournal)
                    VoronUnrecoverableErrorException.Raise(_env,
                        $"PTT of journal {journalFile.Number} already contains records for a new write tx. " +
                        $"Tx id = {id}, max id in journal = {maxTxIdInJournal}");
            }
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
                _root = new Tree(this, null, Constants.RootTreeNameSlice, _state.Root);
            }
        }

        private void InitTransactionHeader()
        {
            Allocator.Allocate(sizeof(TransactionHeader), out _txHeaderMemory);
            Memory.Set(_txHeaderMemory.Ptr, 0, sizeof(TransactionHeader));

            _txHeader = (TransactionHeader*)_txHeaderMemory.Ptr;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Page ModifyPage(long num)
        {
            _env.Options.AssertNoCatastrophicFailure();

            if (_pageLocator.TryGetWritablePage(num, out Page result))
                return result;

            return ModifyPageInternal(num);
        }

        private Page ModifyPageInternal(long num)
        {
            Page currentPage = GetPage(num);

            // Check if we can hit the second level locality cache.            
            if (_dirtyPages.Contains(num))
                return currentPage;                

            // No cache hits.
            int pageSize;
            Page newPage;
            if (currentPage.IsOverflow)
            {
                int numberOfAllocatedPages;
                newPage = AllocateOverflowRawPage(currentPage.OverflowSize, out numberOfAllocatedPages, num, currentPage, zeroPage: false);
                pageSize = Constants.Storage.PageSize * numberOfAllocatedPages;
                _numberOfModifiedPages += numberOfAllocatedPages;
            }
            else
            {
                newPage = AllocatePage(1, num, currentPage, zeroPage: false); // allocate new page in a log file but with the same number			
                pageSize = Environment.Options.PageSize;
                _numberOfModifiedPages += 1;
            }

            Memory.Copy(newPage.Pointer, currentPage.Pointer, pageSize);

            TrackWritablePage(newPage);

            _pageLocator.SetWritable(num, newPage);

            return newPage;
        }

        private const int InvalidScratchFile = -1;
        private PagerStateCacheItem _lastScratchFileUsed = new PagerStateCacheItem(InvalidScratchFile, null);
        private bool _disposed;

        public sealed class PagerRef
        {
            public AbstractPager Pager;
            public long PagerPageNumber;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Page GetPage(long pageNumber)
        {
            if (_disposed)
                ThrowObjectDisposed();

            if (_pageLocator.TryGetReadOnlyPage(pageNumber, out Page result))
                return result;

            return GetPageInternal(pageNumber);
        }

        private Page GetPageInternal(long pageNumber)
        {
            // Check if we can hit the lowest level locality cache.
            Page p;
            PageFromScratchBuffer value;
            if (_scratchPagesTable != null && _scratchPagesTable.TryGetValue(pageNumber, out value)) // Scratch Pages Table will be null in read transactions
            {
                Debug.Assert(value != null);
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
                Debug.Assert(p.PageNumber == pageNumber, string.Format("Requested ReadOnly page #{0}. Got #{1} from scratch", pageNumber, p.PageNumber));
            }
            else
            {
                var pageFromJournal = _journal.ReadPage(this, pageNumber, _scratchPagerStates, null);
                if (pageFromJournal != null)
                {
                    p = pageFromJournal.Value;
                    Debug.Assert(p.PageNumber == pageNumber,
                        string.Format("Requested ReadOnly page #{0}. Got #{1} from journal", pageNumber, p.PageNumber));
                }
                else
                {
                    p = DataPager.ReadPage(this, pageNumber);

                    Debug.Assert(p.PageNumber == pageNumber,
                        string.Format("Requested ReadOnly page #{0}. Got #{1} from data file", pageNumber, p.PageNumber));

                    // When encryption is off, we do validation by checksum
                    if (_env.Options.EncryptionEnabled == false)
                        _env.ValidatePageChecksum(pageNumber, (PageHeader*)p.Pointer);
                }
            }

            TrackReadOnlyPage(p);

            _pageLocator.SetReadable(p.PageNumber, p);
            return p;
        }

        public Page GetPage(long pageNumber, PagerRef pagerRef)
        {
            // Check if we can hit the lowest level locality cache.
            Page p;
            PageFromScratchBuffer value;
            if (_scratchPagesTable != null && _scratchPagesTable.TryGetValue(pageNumber, out value)) // Scratch Pages Table will be null in read transactions
            {
                Debug.Assert(value != null);
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

                p = _env.ScratchBufferPool.ReadPage(this, value.ScratchFileNumber, value.PositionInScratchBuffer, state, pagerRef);
                Debug.Assert(p.PageNumber == pageNumber, string.Format("Requested ReadOnly page #{0}. Got #{1} from scratch", pageNumber, p.PageNumber));
            }
            else
            {
                var pageFromJournal = _journal.ReadPage(this, pageNumber, _scratchPagerStates, pagerRef);
                if (pageFromJournal != null)
                {
                    p = pageFromJournal.Value;
                    Debug.Assert(p.PageNumber == pageNumber,
                        string.Format("Requested ReadOnly page #{0}. Got #{1} from journal", pageNumber, p.PageNumber));
                }
                else
                {
                    p = DataPager.ReadPage(this, pageNumber);
                    if (pagerRef != null)
                    {
                        pagerRef.Pager = DataPager;
                        pagerRef.PagerPageNumber = pageNumber;
                    }

                    Debug.Assert(p.PageNumber == pageNumber,
                        string.Format("Requested ReadOnly page #{0}. Got #{1} from data file", pageNumber, p.PageNumber));

                    // When encryption is off, we do validation by checksum
                    if (_env.Options.EncryptionEnabled == false)
                        _env.ValidatePageChecksum(pageNumber, (PageHeader*)p.Pointer);
                }
            }

            TrackReadOnlyPage(p);

            return p;
        }

        private static void ThrowObjectDisposed()
        {
            throw new ObjectDisposedException("Transaction");
        }

        public Page AllocatePage(int numberOfPages, long? pageNumber = null, Page? previousPage = null, bool zeroPage = true)
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
            return AllocatePage(numberOfPages, pageNumber.Value, previousPage, zeroPage);
        }

        public Page AllocateOverflowRawPage(long overflowSize, out int numberOfPages, long? pageNumber = null, Page? previousPage = null, bool zeroPage = true)
        {
            if (overflowSize > int.MaxValue - 1)
                throw new InvalidOperationException($"Cannot allocate chunks bigger than { int.MaxValue / 1024 * 1024 } Mb.");

            Debug.Assert(overflowSize >= 0);

            numberOfPages = DataPager.GetNumberOfOverflowPages(overflowSize);

            var overflowPage = AllocatePage(numberOfPages, pageNumber, previousPage, zeroPage);
            overflowPage.Flags = PageFlags.Overflow;
            overflowPage.OverflowSize = (int)overflowSize;

            return overflowPage;
        }

        private Page AllocatePage(int numberOfPages, long pageNumber, Page? previousVersion, bool zeroPage)
        {
            if (_disposed)
                throw new ObjectDisposedException("Transaction");

            var maxAvailablePageNumber = _env.Options.MaxStorageSize / Constants.Storage.PageSize;

            if (pageNumber > maxAvailablePageNumber)
                ThrowQuotaExceededException(pageNumber, maxAvailablePageNumber);


            Debug.Assert(pageNumber < State.NextPageNumber);

#if VALIDATE
            VerifyNoDuplicateScratchPages();
#endif
            var pageFromScratchBuffer = _env.ScratchBufferPool.Allocate(this, numberOfPages);
            pageFromScratchBuffer.PreviousVersion = previousVersion;
            _transactionPages.Add(pageFromScratchBuffer);

            _numberOfModifiedPages += numberOfPages;

            _scratchPagesTable[pageNumber] = pageFromScratchBuffer;

            _dirtyPages.Add(pageNumber);

            TrackDirtyPage(pageNumber);

            if (numberOfPages > 1)
                _dirtyOverflowPages.Add(pageNumber + 1, numberOfPages - 1);

            if (numberOfPages != 1)
            {
                _env.ScratchBufferPool.EnsureMapped(this,
                    pageFromScratchBuffer.ScratchFileNumber,
                    pageFromScratchBuffer.PositionInScratchBuffer,
                    numberOfPages);
            }
            
            var newPagePointer = _env.ScratchBufferPool.AcquirePagePointerForNewPage(this, pageFromScratchBuffer.ScratchFileNumber,
                pageFromScratchBuffer.PositionInScratchBuffer, numberOfPages);

            if (zeroPage)
                Memory.Set(newPagePointer, 0, Constants.Storage.PageSize * numberOfPages);

            var newPage = new Page(newPagePointer)
            {
                PageNumber = pageNumber,
                Flags = PageFlags.Single
            };

            TrackWritablePage(newPage);

#if VALIDATE
            VerifyNoDuplicateScratchPages();
#endif

            return newPage;
        }

        private void ThrowQuotaExceededException(long pageNumber, long? maxAvailablePageNumber)
        {
            throw new QuotaException(
                string.Format(
                    "The maximum storage size quota ({0} bytes) has been reached. " +
                    "Currently configured storage quota is allowing to allocate the following maximum page number {1}, while the requested page number is {2}. " +
                    "To increase the quota, use the MaxStorageSize property on the storage environment options.",
                    _env.Options.MaxStorageSize, maxAvailablePageNumber, pageNumber));
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
            _env.ScratchBufferPool.BreakLargeAllocationToSeparatePages(this, value);

            for (int i = 0; i < value.NumberOfPages; i++)
            {
                var pageFromScratchBuffer = new PageFromScratchBuffer(value.ScratchFileNumber, value.PositionInScratchBuffer + i, 1, 1);
                _transactionPages.Add(pageFromScratchBuffer);
                _scratchPagesTable[pageNumber + i] = pageFromScratchBuffer;
                _dirtyOverflowPages.Remove(pageNumber + i);
                _dirtyPages.Add(pageNumber + i);
                TrackDirtyPage(pageNumber + i);
                var newPage = _env.ScratchBufferPool.ReadPage(this, value.ScratchFileNumber, value.PositionInScratchBuffer + i);
                newPage.PageNumber = pageNumber + i;
                newPage.Flags = PageFlags.Single;
                TrackWritablePage(newPage);
            }
        }

        internal void ShrinkOverflowPage(long pageNumber, int newSize)
        {
            if (_disposed)
                throw new ObjectDisposedException("Transaction");

            PageFromScratchBuffer value;
            if (_scratchPagesTable.TryGetValue(pageNumber, out value) == false)
                throw new InvalidOperationException("The page " + pageNumber + " was not previous allocated in this transaction");

            var page = _env.ScratchBufferPool.ReadPage(this, value.ScratchFileNumber, value.PositionInScratchBuffer);
            if (page.IsOverflow == false || page.OverflowSize < newSize)
                throw new InvalidOperationException("The page " + pageNumber +
                                                    " was is not an overflow page greater than " + newSize);

            var prevNumberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize);
            page.OverflowSize = newSize;
            var lowerNumberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(newSize);

            if (prevNumberOfPages == lowerNumberOfPages)
                return;

            for (int i = lowerNumberOfPages; i < prevNumberOfPages; i++)
            {
                FreePage(page.PageNumber + i);
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
        public NativeMemory.ThreadStats CurrentTransactionHolder { get; set; }

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
            if (JournalFiles != null)
            {
                foreach (var journalFile in JournalFiles)
                {
                    journalFile.Release();
                }
            }

            _root?.Dispose();
            _freeSpaceTree?.Dispose();

            if (_disposeAllocator)
                _allocator.Dispose();



            OnDispose?.Invoke(this);
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

            _pageLocator.Reset(pageNumber); // Remove it from the page locator.

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

            if (_dirtyPages.Remove(pageNumber) == false &&
                _dirtyOverflowPages.TryGetValue(pageNumber, out numberOfOverflowPages))
            {
                _dirtyOverflowPages.Remove(pageNumber);

                if (numberOfOverflowPages > 1) // prevent adding range which length is 0
                    _dirtyOverflowPages.Add(pageNumber + 1, numberOfOverflowPages - 1); // change the range of the overflow page
            }

            UntrackDirtyPage(pageNumber);
        }


        private class PagerStateCacheItem
        {
            public readonly int FileNumber;
            public readonly PagerState State;

            public PagerStateCacheItem(int file, PagerState state)
            {
                FileNumber = file;
                State = state;
            }
        }


        public void Commit()
        {
            if (Flags != TransactionFlags.ReadWrite)
                return;// nothing to do

            CommitStage1_CompleteTransaction();

            if (WriteToJournalIsRequired())
            {
                Environment.LastWorkTime = DateTime.UtcNow;
                CommitStage2_WriteToJournal();
            }

            BeforeCommitFinalization?.Invoke(this);
            CommitStage3_DisposeTransactionResources();
        }

        internal Task AsyncCommit;
        private LowLevelTransaction _asyncCommitNextTransaction;

        /// <summary>
        /// This begins an async commit and starts a new transaction immediately.
        /// The current transaction is considered completed in memory but not yet
        /// committed to disk. This *must* be completed by calling EndAsyncCommit.
        /// </summary>
        public LowLevelTransaction BeginAsyncCommitAndStartNewTransaction()
        {
            if (Flags != TransactionFlags.ReadWrite)
                ThrowReadTranscationCannotDoAsyncCommit();
            if (_asyncCommitNextTransaction != null)
                ThrowAsyncCommitAlreadyCalled();

            // we have to check the state before we complete the transaction
            // because that would change whether we need to write to the journal
            var writeToJournalIsRequired = WriteToJournalIsRequired();

            CommitStage1_CompleteTransaction();

            var nextTx = new LowLevelTransaction(this);
            _asyncCommitNextTransaction = nextTx;
            AsyncCommit = writeToJournalIsRequired
                  ? Task.Run(() => { CommitStage2_WriteToJournal(); })
                  : Task.CompletedTask;

            try
            {
                _env.IncrementUsageOnNewTransaction();
                _env.ActiveTransactions.Add(nextTx);
                _env.WriteTransactionStarted();

                return nextTx;
            }
            catch (Exception)
            {
                // failure here means that we'll try to complete the current transaction normaly
                // then throw as if commit was called normally and the next transaction failed

                _env.DecrementUsageOnTransactionCreationFailure();

                EndAsyncCommit();

                AsyncCommit = null;

                throw;
            }
        }

        private static void ThrowAsyncCommitAlreadyCalled()
        {
            throw new InvalidOperationException("Cannot start a new async commit because one was already started");
        }

        private static void ThrowReadTranscationCannotDoAsyncCommit()
        {
            throw new InvalidOperationException("Only write transactions can do async commit");
        }

        /// <summary>
        /// Completes the async commit began previously. Must be called *within* the 
        /// write lock, and must happen *before* the new transaction call its own commit
        /// method.
        /// </summary>
        public void EndAsyncCommit()
        {
            if (AsyncCommit == null)
            {
                ThrowInvalidAsyncEndWithoutBegin();
                return;// never reached
            }

            AsyncCommit.Wait();
            Environment.LastWorkTime = DateTime.UtcNow;
            CommitStage3_DisposeTransactionResources();
            BeforeCommitFinalization?.Invoke(this);
        }

        private static void ThrowInvalidAsyncEndWithoutBegin()
        {
            throw new InvalidOperationException("Cannot call EndAsyncCommit when we don't have an async op running");
        }

        private bool WriteToJournalIsRequired()
        {
            return _dirtyPages.Count > 0
                   || _dirtyOverflowPages.Count > 0
                   || _freedPages.Count > 0
                   // nothing changed in this transaction
                   // allow call to writeToJournal for flushing lazy tx
                   || (IsLazyTransaction == false && _journal?.HasDataInLazyTxBuffer() == true);
        }

        private void CommitStage2_WriteToJournal()
        {
            // In the case of non-lazy transactions, we must flush the data from older lazy transactions
            // to ensure the sequentiality of the data.
            var numberOfWrittenPages = _journal.WriteToJournal(this);
            FlushedToJournal = true;

            if (_requestedCommitStats != null)
            {
                _requestedCommitStats.NumberOfModifiedPages = numberOfWrittenPages.NumberOfUncompressedPages;
                _requestedCommitStats.NumberOf4KbsWrittenToDisk = numberOfWrittenPages.NumberOf4Kbs;
            }
        }

        private void CommitStage1_CompleteTransaction()
        {
            if (_disposed)
                ThrowObjectDisposed();

            if (Committed)
                ThrowAlreadyCommitted();

            if (RolledBack)
                ThrowAlreadyRolledBack();

            while (_pagesToFreeOnCommit.Count > 0)
            {
                FreePage(_pagesToFreeOnCommit.Pop());
            }
            _txHeader->LastPageNumber = _state.NextPageNumber - 1;
            _state.Root.CopyTo(&_txHeader->Root);

            _txHeader->TxMarker |= TransactionMarker.Commit;
        }

        private void CommitStage3_DisposeTransactionResources()
        {
            // an exception being throw after the transaction has been committed to disk 
            // will corrupt the in memory state, and require us to restart (and recover) to 
            // be in a valid state
            try
            {
                ValidateAllPages();

                Allocator.Release(ref _txHeaderMemory);                

                Committed = true;
                _env.TransactionAfterCommit(this);

                if (_asyncCommitNextTransaction != null)
                {
                    var old = _asyncCommitNextTransaction.JournalFiles;
                    _asyncCommitNextTransaction.JournalFiles = _env.Journal.Files;
                    foreach (var journalFile in _asyncCommitNextTransaction.JournalFiles)
                    {
                        journalFile.AddRef();
                    }
                    foreach (var journalFile in old)
                    {
                        journalFile.Release();
                    }
                }

                // Releasing the page locator is the last thing we do. 
                PersistentContext.FreePageLocator(_pageLocator);
            }
            catch (Exception e)
            {
                _env.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));

                throw;
            }
        }

        private static void ThrowAlreadyRolledBack()
        {
            throw new InvalidOperationException("Cannot commit rolled-back transaction.");
        }

        private static void ThrowAlreadyCommitted()
        {
            throw new InvalidOperationException("Cannot commit already committed transaction.");
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
                _env.ScratchBufferPool.Free(pageFromScratch.ScratchFileNumber, pageFromScratch.PositionInScratchBuffer, null);
            }

            foreach (var pageFromScratch in _unusedScratchPages)
            {
                _env.ScratchBufferPool.Free(pageFromScratch.ScratchFileNumber, pageFromScratch.PositionInScratchBuffer, null);
            }

            // release scratch file page allocated for the transaction header
            Allocator.Release(ref _txHeaderMemory);

            using (_env.PreventNewReadTransactions())
            {
                _env.ScratchBufferPool.UpdateCacheForPagerStatesOfAllScratches();
                _env.Journal.UpdateCacheForJournalSnapshots();
            }

            RolledBack = true;
        }
        public void RetrieveCommitStats(out CommitStats stats)
        {
            _requestedCommitStats = stats = new CommitStats();
        }

        private PagerState _lastState;
        private bool _isLazyTransaction;

        internal ActiveTransactions.Node ActiveTransactionNode;
        internal bool FlushInProgressLockTaken;
        private ByteString _txHeaderMemory;
        internal ImmutableAppendOnlyList<JournalFile> JournalFiles;
        internal bool AlreadyAllowedDisposeWithLazyTransactionRunning;
        public DateTime TxStartTime;
        
        public void EnsurePagerStateReference(PagerState state)
        {
            if (state == _lastState || state == null)
                return;

            _lastState = state;
            if (_pagerStates.Add(state) == false)
                return;
            state.AddRef();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void OnAfterCommitWhenNewReadTransactionsPrevented()
        {
            // the event cannot be called outside this class while we need to call it in 
            // StorageEnvironment.TransactionAfterCommit
            AfterCommitWhenNewReadTransactionsPrevented?.Invoke();
        }


#if VALIDATE_PAGES

        private Dictionary<long, ulong> readOnlyPages = new Dictionary<long, ulong>();
        private Dictionary<long, ulong> writablePages = new Dictionary<long, ulong>();
        private readonly HashSet<long> dirtyPagesValidate = new HashSet<long>();

        private void ValidateAllPages()
        {
            ValidateWritablePages();
            ValidateReadOnlyPages();
        }

        private void ValidateReadOnlyPages()
        {
            foreach (var readOnlyKey in readOnlyPages)
            {
                long pageNumber = readOnlyKey.Key;
                if (dirtyPagesValidate.Contains(pageNumber))
                    VoronUnrecoverableErrorException.Raise(_env, "Read only page is dirty (which means you are modifying a page directly in the data -- non transactionally -- ).");

                var page = GetPage(pageNumber);
                
                ulong pageHash = _env.CalculatePageChecksum(page.Pointer, page.PageNumber, page.Flags, page.OverflowSize);
                if (pageHash != readOnlyKey.Value)
                    VoronUnrecoverableErrorException.Raise(_env, "Read only page content is different (which means you are modifying a page directly in the data -- non transactionally -- ).");
            }
        }

        private void ValidateWritablePages()
        {
            foreach (var writableKey in writablePages)
            {
                long pageNumber = writableKey.Key;
                if (!dirtyPagesValidate.Contains(pageNumber))
                    VoronUnrecoverableErrorException.Raise(_env, "Writable key is not dirty (which means you are asking for a page modification for no reason).");
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

            if (writablePages.ContainsKey(page.PageNumber) == false)
            {
                ulong pageHash = _env.CalculatePageChecksum(page.Pointer, page.PageNumber, page.Flags, page.OverflowSize);
                writablePages[page.PageNumber] = pageHash;
            }
        }

        private void TrackReadOnlyPage(Page page)
        {
            if (writablePages.ContainsKey(page.PageNumber))
                return;

            ulong pageHash = _env.CalculatePageChecksum(page.Pointer, page.PageNumber, page.Flags, page.OverflowSize);

            ulong storedHash;
            if (readOnlyPages.TryGetValue(page.PageNumber, out storedHash))
            {
                if (pageHash != storedHash)
                    VoronUnrecoverableErrorException.Raise(_env, "Read Only Page has change between tracking requests. Page #" + page.PageNumber);
            }
            else
            {
                readOnlyPages[page.PageNumber] = pageHash;
            }
        }

        private void TrackDirtyPage(long page)
        {
            dirtyPagesValidate.Add(page);
        }

        private void UntrackDirtyPage(long page)
        {
            dirtyPagesValidate.Remove(page);
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

        [Conditional("VALIDATE_PAGES")]
        private void TrackDirtyPage(long page) { }

        [Conditional("VALIDATE_PAGES")]
        private void UntrackDirtyPage(long page) { }
#endif

        internal TransactionHeader* GetTransactionHeader()
        {
            return _txHeader;
        }
    }
}