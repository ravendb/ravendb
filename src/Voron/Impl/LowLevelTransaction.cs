using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using CollectionsMarshal = System.Runtime.InteropServices.CollectionsMarshal;
using System.Threading;
using System.Threading.Tasks;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Collections;
using Sparrow.Threading;
using Sparrow.Utils;
using Sparrow.Server.Utils;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Voron.Debugging;
using Voron.Util;
using static Sparrow.DisposableExceptions;
using static Sparrow.PortableExceptions;

#if DEBUG
using System.Linq; // Needed in DEBUG
using System.Text; // Needed in DEBUG
#endif

using Constants = Voron.Global.Constants;

namespace Voron.Impl
{
    public sealed unsafe class LowLevelTransaction : IDisposable, IDisposableQueryable, INotifyAllocationFailure
    {
        public readonly Pager DataPager;
        private readonly StorageEnvironment _env;
        private readonly ByteStringContext _allocator;
        internal PageLocator _pageLocator;
        private readonly bool _disposeAllocator;
        private readonly bool _isValidationEnabled;
        private ScratchPagesTable _scratchPagesInUse;
        private readonly ScratchPagesSnapshot _scratchPagesForReads;
        private HashSet<long> _sparsePageRanges;
        private readonly GetPageMethod _getPageMethod;
        private readonly long _id = -1; // 0 is a valid value, so we need to use -1 to indicate that it is not set yet
        internal long ScratchSnapshotSeq = -1; // 0 is a valid value, so we need to use -1 to indicate that it is not set yet

        internal long DecompressedBufferBytes;
        internal TestingStuff _forTestingPurposes;
        private bool _updateLastWorkTime = true;
        public Pager.State DataPagerState;

        private enum GetPageMethod
        {
            WriteScratchFirst,
            ReadScratchFirst,
            DataFile
        }

        private Tree _root;
        public Tree RootObjects => _root;

        public long WrittenToJournalNumber = -1;

        private long _numberOfModifiedPages;

        public long NumberOfModifiedPages => _numberOfModifiedPages;

        public Pager.PagerTransactionState PagerTransactionState;
        private readonly WriteAheadJournal _journal;

        public ScratchPagesSnapshot ScratchTableSnapshot;


        internal sealed class WriteTransactionPool
        {
#if DEBUG
            public int BuilderUsages;
#endif
            public readonly TableValueBuilder TableValueBuilder = new();

            public readonly HashSet<long> DirtyPagesPool = new();

            public void Reset()
            {
                DirtyPagesPool.Clear();
                TableValueBuilder.Reset();
            }
        }

        // BEGIN: Structures that are safe to pool.
        private readonly HashSet<long> _dirtyPages;
        private readonly Stack<long> _pagesToFreeOnCommit;
        // END: Structures that are safe to pool.


        public event Action<LowLevelTransaction> BeforeCommitFinalization;

        public event Action<LowLevelTransaction> LastChanceToReadFromWriteTransactionBeforeCommit;

        public Size TransactionSize => new Size(NumberOfModifiedPages * Constants.Storage.PageSize, SizeUnit.Bytes) + AdditionalMemoryUsageSize;

        public Size AdditionalMemoryUsageSize
        {
            get
            {
                var additionalMemoryUsageSize = PagerTransactionState.AdditionalMemoryUsageSize;
                additionalMemoryUsageSize.Add(DecompressedBufferBytes, SizeUnit.Bytes);
                return additionalMemoryUsageSize;
            }
        }
        public event Action<LowLevelTransaction> OnDispose;

        /// <summary>
        /// This is called *under the write transaction lock* and will
        /// allow us to clean up any in memory state that shouldn't be preserved
        /// passed the transaction rollback
        /// </summary>
        public event Action<LowLevelTransaction> OnRollBack;

        private readonly IFreeSpaceHandling _freeSpaceHandling;
        internal FixedSizeTree _freeSpaceTree;

        private TransactionHeader _txHeader;

        internal ref TransactionHeader TransactionHeader => ref _txHeader;

        private readonly List<PageFromScratchBuffer> _transactionPages;
        private readonly Dictionary<long, int> _transactionPagesIndex;
        private int _pagesCountIncludingAllOverflowPages;
        private bool _hasFreePages;
        private HashSet<long> _freedPages;

        private CommitStats _requestedCommitStats;

        public TransactionPersistentContext PersistentContext { get; }
        public TransactionFlags Flags { get; }

        public StorageEnvironment Environment => _env;

        public long Id => _id;

        public bool Committed { get; private set; }

        public bool RolledBack { get; private set; }

        public ByteStringContext Allocator => _allocator;

        public ulong Hash => _txHeader.Hash;

        public LowLevelTransaction(LowLevelTransaction previous, TransactionPersistentContext transactionPersistentContext, ByteStringContext allocator)
        {
            // this is used to clone a read transaction, so we can dispose the old one.
            // for example, in 32 bits, we may want to have a large transaction, but we 
            // can't keep around and scanning the whole db. So we clone the transaction,
            // create a new one, and release all the mappings associated with the old one
            // the new transaction will create its own mapping, using the existing page translation
            // table and install itself as the same transaction id as the current one. 

            Debug.Assert(previous.Flags is TransactionFlags.Read);

            IsCloned = true;

            PersistentContext = transactionPersistentContext;

            TxStartTime = previous.TxStartTime;
            DataPager = previous.DataPager;
            DataPagerState = previous.DataPagerState;

            _txHeader = TxHeaderInitializerTemplate;
            _env = previous._env;
            _journal = previous._journal;
            _id = previous._id;
            _envRecord = previous._envRecord;
            _freeSpaceHandling = previous._freeSpaceHandling;
            _allocator = allocator ?? new ByteStringContext(SharedMultipleUseFlag.None);
            _allocator.RegisterListener(this);
            _disposeAllocator = allocator == null;
            _isValidationEnabled = _env.Options.Encryption.IsEnabled == false;
            _scratchPagesForReads = previous._scratchPagesForReads;
            ScratchSnapshotSeq = previous.ScratchSnapshotSeq;
            _getPageMethod = previous._getPageMethod;

            Flags = TransactionFlags.Read;

            _env.ActiveTransactions.Add(this);

            try
            {
                _pageLocator = AllocatePageLocator();

                InitializeRoots();
            }
            catch
            {
                _env.ActiveTransactions.TryRemove(this);
                throw;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PageLocator AllocatePageLocator()
        {
            return PersistentContext.AllocatePageLocator();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FreePageLocator(TransactionPersistentContext persistentContext, ref PageLocator locator)
        {
            if(locator == null)
                return;
            persistentContext.FreePageLocator(locator);
            locator = null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FreePageLocator(ref PageLocator locator)
        {
            FreePageLocator(PersistentContext, ref locator);
        }

        private LowLevelTransaction(LowLevelTransaction previous, TransactionPersistentContext persistentContext, long txId)
        {
            // this is meant to be used with transaction merging only
            // so it makes a lot of assumptions about the usage scenario
            // and what it can do

            Debug.Assert(previous.Flags is TransactionFlags.ReadWrite);

            PagerTransactionState.IsWriteTransaction = true;
            var env = previous._env;
            env.Options.AssertNoCatastrophicFailure();

            if (VoronConfiguration.FailFastForStability && env.Options.Encryption.IsEnabled)
                VoronUnrecoverableErrorException.Raise(env, $"Async commit isn't supported in encrypted environments. We don't carry encrypted state from previous tx");
            if (VoronConfiguration.FailFastForStability && (PlatformDetails.Is32Bits || env.Options.ForceUsing32BitsPager))
                VoronUnrecoverableErrorException.Raise(env, $"Async commit isn't supported in 32bits environments. We don't carry 32 bits state from previous tx");

            CurrentTransactionIdHolder = previous.CurrentTransactionIdHolder;
            TxStartTime = DateTime.UtcNow;
            DataPager = previous.DataPager;
            DataPagerState = previous.DataPagerState;
            _id = txId;
            _envRecord = previous._envRecord with
            {
                TransactionId = _id,
                Root = previous._root.ReadHeader()
            };
            _localTxNextPageNumber = previous._localTxNextPageNumber;

            _txHeader = TxHeaderInitializerTemplate;
            _env = env;
            _journal = env.Journal;
            _freeSpaceHandling = previous._freeSpaceHandling;

            if (VoronConfiguration.FailFastForStability && persistentContext == null)
                VoronUnrecoverableErrorException.Raise(env, $"{nameof(persistentContext)} != null");

            PersistentContext = persistentContext;

            _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
            _disposeAllocator = true;
            _allocator.RegisterListener(this);
            _isValidationEnabled = _env.Options.Encryption.IsEnabled == false;
            _scratchPagesInUse = previous._scratchPagesInUse;
            ScratchSnapshotSeq = env.CurrentStateRecord.ScratchPagesTable.VisibleAsOfSeq;
            _scratchPagesInUse.BeginWriteTransaction(ScratchSnapshotSeq);
            _getPageMethod = GetPageMethod.WriteScratchFirst;

            Flags = TransactionFlags.ReadWrite;

            _dirtyPages = previous._dirtyPages;
            _dirtyPages.Clear();

            _transactionPages = new List<PageFromScratchBuffer>();
            _transactionPagesIndex = new Dictionary<long, int>();
            _pagesToFreeOnCommit = new Stack<long>();

            _pageLocator = AllocatePageLocator();

            InitializeRoots();
            InitTransactionHeader();
        }

        public LowLevelTransaction(StorageEnvironment env, TransactionPersistentContext transactionPersistentContext, TransactionFlags flags, IFreeSpaceHandling freeSpaceHandling, ByteStringContext context = null)
        {
            TxStartTime = DateTime.UtcNow;

            if (flags == TransactionFlags.ReadWrite)
            {
                env.Options.AssertNoCatastrophicFailure();
                PagerTransactionState.IsWriteTransaction = true;
            }

            _env = env;
            PersistentContext = transactionPersistentContext;
            Flags = flags;

            var provisionalRecord = env.CurrentStateRecord;
            // we must set the _id and ScratchSnapshotSeq to valid values *before* adding to ActiveTransactions. 
            // Afterward, we may move to a _later_ state (which is fine)
            _id = flags == TransactionFlags.ReadWrite ? provisionalRecord.TransactionId + 1 : provisionalRecord.TransactionId;
            ScratchSnapshotSeq = provisionalRecord.ScratchPagesTable.VisibleAsOfSeq;

            env.ActiveTransactions.Add(this);

            try
            {
                _envRecord = env.CurrentStateRecord;
                DataPagerState = _envRecord.DataPagerState;
                DataPager = env.DataPager;

                _journal = env.Journal;
                _freeSpaceHandling = freeSpaceHandling;

                _allocator = context ?? new ByteStringContext(SharedMultipleUseFlag.None);
                _allocator.RegisterListener(this);
                _disposeAllocator = context == null;

                _isValidationEnabled = _env.Options.Encryption.IsEnabled == false;

                _pageLocator = transactionPersistentContext.AllocatePageLocator();

                if (flags != TransactionFlags.ReadWrite)
                {
                    _id = _envRecord.TransactionId;
                    _scratchPagesForReads = _envRecord.ScratchPagesTable;
                    ScratchSnapshotSeq = _scratchPagesForReads.VisibleAsOfSeq;
                    _getPageMethod = _scratchPagesForReads.Count > 0 ? GetPageMethod.ReadScratchFirst : GetPageMethod.DataFile;
                    InitializeRoots();

                    return;
                }

                Debug.Assert(_envRecord.TransactionId == provisionalRecord.TransactionId,
                    "the write lock serializes commits, so the record cannot advance under a write transaction");
                _envRecord = _envRecord with { TransactionId = _id };
                _getPageMethod = GetPageMethod.WriteScratchFirst;
                _env.WriteTransactionPool.Reset();
                _dirtyPages = _env.WriteTransactionPool.DirtyPagesPool;
                _scratchPagesInUse = _env.ScratchPagesTable;
                ScratchSnapshotSeq = _envRecord.ScratchPagesTable.VisibleAsOfSeq;
                _scratchPagesInUse.BeginWriteTransaction(ScratchSnapshotSeq);
                _transactionPages = new List<PageFromScratchBuffer>();
                _transactionPagesIndex = new Dictionary<long, int>();
                _pagesToFreeOnCommit = new Stack<long>();

                InitializeRoots();
                InitTransactionHeader();
            }
            catch
            {
                env.ActiveTransactions.TryRemove(this);
                throw;
            }
        }

        internal EnvironmentStateRecord CurrentStateRecord => _envRecord;

        public bool TryGetClientState<T>(out T value)
        {
            var envRecord = _envRecord;

            if (envRecord is null)
                ThrowObjectDisposed();

            if (envRecord.ClientState is T t)
            {
                value = t;
                return true;
            }

            value = default;
            return false;
        }

        internal void UpdateRootsIfNeeded(Tree root)
        {
            //can only happen during initial transaction that creates Root and FreeSpaceRoot trees
            if (_envRecord.Root.RootObjectType is not RootObjectType.None)
                return;

            _envRecord = _envRecord with { Root = root.ReadHeader() };

            _root = root;
        }

        internal void UpdateDataPagerState(Pager.State dataPagerState)
        {
            if (VoronConfiguration.FailFastForStability && Flags is TransactionFlags.Read)
                VoronUnrecoverableErrorException.Raise(this, "Flags is TransactionFlags.Read");

            DataPagerState = dataPagerState;
        }

        internal void UpdateJournal(long journalNumber, long last4KWrite)
        {
            _envRecord = _envRecord with { Journal = (journalNumber, last4KWrite) };
        }

        internal void UpdateClientState(object state)
        {
            if (VoronConfiguration.FailFastForStability)
            {
                if ((_envRecord.ClientState == null || state == null || _envRecord.ClientState.GetType() == state.GetType()) == false)
                    VoronUnrecoverableErrorException.Raise(this, $"Cannot *change* the type of the client state, must always be a single type! Was {_envRecord.ClientState?.GetType()} to {state?.GetType()}");
            }

            _envRecord = _envRecord with { ClientState = state };
        }

        private void InitializeRoots()
        {
            if (_root == null &&
                _envRecord.Root.RootObjectType is not RootObjectType.None)
            {
                _root = Tree.GetRoot(this, Constants.RootTreeNameSlice, _envRecord.Root);
            }
        }

        private static readonly TransactionHeader TxHeaderInitializerTemplate = new()
        {
            HeaderMarker = Constants.TransactionHeaderMarker,
            LastPageNumber = -1,
            PageCount = -1,
            TxMarker = TransactionMarker.None,
            CompressedSize = 0,
            UncompressedSize = 0
        };

        private void InitTransactionHeader()
        {
            long nextPageNumber = GetNextPageNumber();
            if (_envRecord.TransactionId > 1 && nextPageNumber <= 1)
                ThrowNextPageNumberCannotBeSmallerOrEqualThanOne();

            _txHeader = TxHeaderInitializerTemplate;
            _txHeader.HeaderMarker = Constants.TransactionHeaderMarker;

            _txHeader.TransactionId = _envRecord.TransactionId;
            _txHeader.NextPageNumber = nextPageNumber;
            _txHeader.TimeStampTicksUtc = DateTime.UtcNow.Ticks;
        }

        internal List<PageFromScratchBuffer> GetTransactionPages()
        {
            VerifyNoDuplicateScratchPages();
            return _transactionPages;
        }

        internal int PagesCountIncludingAllOverflowPages
        {
            get
            {
                if (_pagesCountIncludingAllOverflowPages < _transactionPages.Count)
                    ThrowPagesCountIncludingAllOverflowPagesIsBroken();
                return _pagesCountIncludingAllOverflowPages;
            }
        }

        [DoesNotReturn]
        private void ThrowPagesCountIncludingAllOverflowPagesIsBroken()
        {
            throw new InvalidOperationException(
                $"Transaction {Id} holds {_transactionPages.Count} scratch pages, but their count including overflows is {_pagesCountIncludingAllOverflowPages}. " +
                "The counter can never be below the number of pages, it either overflowed or a mutation of the transaction pages failed to maintain it.");
        }

        [Conditional("DEBUG")]
        internal void VerifyPagesCountIncludingAllOverflowPages()
        {
            var total = 0;
            foreach (ref readonly var page in CollectionsMarshal.AsSpan(_transactionPages))
                total += page.NumberOfPages;

            Debug.Assert(total == _pagesCountIncludingAllOverflowPages,
                $"Transaction {Id}: the transaction pages sum to {total} pages including overflows, but the counter holds {_pagesCountIncludingAllOverflowPages}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long TransactionPageKey(in PageFromScratchBuffer page)
        {
            Debug.Assert(page.PositionInScratchBuffer <= uint.MaxValue, "a scratch position has to fit in the low half of the key");
            return ((long)page.File.Number << 32) | (uint)page.PositionInScratchBuffer;
        }

        private void AddTransactionPage(in PageFromScratchBuffer page)
        {

            _transactionPagesIndex.Add(TransactionPageKey(page), _transactionPages.Count);
            _transactionPages.Add(page);
            _pagesCountIncludingAllOverflowPages += page.NumberOfPages;
        }

        private void RemoveTransactionPage(in PageFromScratchBuffer page)
        {
            if (_transactionPagesIndex.Remove(TransactionPageKey(page), out var index) == false)
                return;

            _pagesCountIncludingAllOverflowPages -= page.NumberOfPages;

            var pages = CollectionsMarshal.AsSpan(_transactionPages);
            var last = pages.Length - 1;
            if (index != last)
            {
                pages[index] = pages[last];
                _transactionPagesIndex[TransactionPageKey(pages[index])] = index;
            }

            _transactionPages.RemoveAt(last);
        }

        private void ReplaceTransactionPage(in PageFromScratchBuffer oldPage, in PageFromScratchBuffer newPage)
        {
            if (_transactionPagesIndex.Remove(TransactionPageKey(oldPage), out var index) == false)
                throw new InvalidOperationException($"Cannot replace transaction page {oldPage.File.Number}:{oldPage.PositionInScratchBuffer} because it is not tracked in the transaction pages.");

            _transactionPagesIndex.Add(TransactionPageKey(newPage), index);
            _transactionPages[index] = newPage;
            _pagesCountIncludingAllOverflowPages += newPage.NumberOfPages - oldPage.NumberOfPages;
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
                newPage = AllocateOverflowRawPage(currentPage.OverflowSize, out var numberOfAllocatedPages, num, currentPage, zeroPage: false);
                pageSize = Constants.Storage.PageSize * numberOfAllocatedPages;
            }
            else
            {
                newPage = AllocatePageImpl(1, num, currentPage, zeroPage: false); // allocate new page in a log file but with the same number			
                pageSize = Environment.Options.PageSize;
            }

            Memory.Copy(newPage.Pointer, currentPage.Pointer, pageSize);

            ZeroPageHeaderChecksumToEnsureNoUseOfCryptoReservedSpace(newPage.Pointer);

            TrackWritablePage(newPage);

            _pageLocator.SetWritable(newPage);

            return newPage;
        }

        [Conditional("DEBUG")]
        private void ZeroPageHeaderChecksumToEnsureNoUseOfCryptoReservedSpace(byte* page)
        {
            if (_env.Options.Encryption.IsEnabled)
            {
                // we don't want to mark all pages as changed by the crypto pager
                // that check for the full page hash, and it isn't relevant for 
                // crypto anyway
                return;
            }

            // in order to ensure that the last 32 bytes of the page are always reserved
            // for crypto, we'll zero them after copying them from the pager. If using 
            // encrypted, we'll always re-write it anyway, and this ensures that we'll
            // get corruption from the system if we try to use it. Note that the normal
            // pager stuff, like ensuring the checksum, is already done at this point
            Sodium.sodium_memzero(page + PageHeader.NonceOffset, (UIntPtr)(PageHeader.SizeOf - PageHeader.NonceOffset));
        }

        private TxStatus _txStatus;

        [Flags]
        private enum TxStatus
        {
            None,
            Disposed = 1,
            Errored = 2
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Page GetPage(long pageNumber)
        {
            if (IsValid == false)
                ThrowObjectDisposed();

            if (_pageLocator.TryGetReadOnlyPage(pageNumber, out Page result))
                return result;

            var p = GetPageInternal(pageNumber);
            if (VoronConfiguration.FailFastForStability && p.PageNumber != pageNumber)
                VoronUnrecoverableErrorException.Raise(this, $"Requested ReadOnly page #{pageNumber}. Got #{p.PageNumber} from data file");

            TrackReadOnlyPage(p);

            _pageLocator.SetReadable(p);

            return p;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Page GetPageWithoutCache(long pageNumber)
        {
            if (IsValid == false)
                ThrowObjectDisposed();

            var p = GetPageInternal(pageNumber);
            if (VoronConfiguration.FailFastForStability && p.PageNumber != pageNumber)
                VoronUnrecoverableErrorException.Raise(this, $"Requested ReadOnly page #{pageNumber}. Got #{p.PageNumber} from data file");

            TrackReadOnlyPage(p);

            return p;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T GetPageHeader<T>(long pageNumber) where T : unmanaged
        {
            if (IsValid == false)
                ThrowObjectDisposed();

            if (_pageLocator.TryGetReadOnlyPage(pageNumber, out Page result))
                return *(T*)result.Pointer;

            return GetPageHeaderInternal<T>(pageNumber);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Page GetPageInternal(long pageNumber)
        {
            switch (_getPageMethod)
            {
                case GetPageMethod.WriteScratchFirst:
                    return PageInternalFromWriteScratchTable(pageNumber);
                case GetPageMethod.ReadScratchFirst:
                    return PageInternalFromReadScratchTable(pageNumber);
                case GetPageMethod.DataFile:
                    return GetPageInternalFromDataFile(pageNumber);
                default:
                    throw new ArgumentOutOfRangeException(_getPageMethod.ToString());
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private T GetPageHeaderInternal<T>(long pageNumber) where T : unmanaged
        {
            switch (_getPageMethod)
            {
                case GetPageMethod.WriteScratchFirst:
                    if (_scratchPagesInUse.TryGetValue(pageNumber, out var pageInUse))
                        return *(T*)pageInUse.ReadWritableRawPagePointer(ref PagerTransactionState);

                    return GetPageHeaderFromDataFile<T>(pageNumber);

                case GetPageMethod.ReadScratchFirst:
                    if (_scratchPagesForReads.TryGetValue(pageNumber, out var readPage))
                        return *(T*)readPage.ReadRawPagePointer(ref PagerTransactionState);

                    return GetPageHeaderFromDataFile<T>(pageNumber);

                case GetPageMethod.DataFile:
                    return GetPageHeaderFromDataFile<T>(pageNumber);

                default:
                    throw new ArgumentOutOfRangeException(_getPageMethod.ToString());
            }
        }

        internal bool TryGetScratchPage(long pageNumber, out PageFromScratchBuffer page) => _scratchPagesInUse.TryGetValue(pageNumber, out page);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Page PageInternalFromWriteScratchTable(long pageNumber)
        {
            if (_scratchPagesInUse.TryGetValue(pageNumber, out var value))
                return value.ReadWritable(this);
            return GetPageInternalFromDataFile(pageNumber);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Page PageInternalFromReadScratchTable(long pageNumber)
        {
            if (_scratchPagesForReads.TryGetValue(pageNumber, out var value))
                return value.ReadPage(this);
            return GetPageInternalFromDataFile(pageNumber);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Page GetPageInternalFromDataFile(long pageNumber)
        {
            var page = new Page(DataPager.AcquirePagePointerWithOverflowHandling(DataPagerState, ref PagerTransactionState, pageNumber));
            if (_isValidationEnabled)// When encryption is off, we do validation by checksum
                _env.ValidatePageChecksum(pageNumber, (PageHeader*)page.Pointer);
            return page;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private T GetPageHeaderFromDataFile<T>(long pageNumber) where T : unmanaged
        {
            return *(T*)DataPager.AcquireRawPagePointer(DataPagerState, ref PagerTransactionState, pageNumber);
        }

        public void TryReleasePage(long pageNumber)
        {
            var inScratches = Flags switch
            {
                TransactionFlags.Read => _scratchPagesForReads.ContainsKey(pageNumber),
                TransactionFlags.ReadWrite => _scratchPagesInUse.ContainsKey(pageNumber),
                _ => throw new ArgumentOutOfRangeException(nameof(Flags))
            };

            if (inScratches) // we don't release pages from the scratch buffers
                return;

            DataPager.TryReleasePage(ref PagerTransactionState, pageNumber);
            
            _pageLocator.Reset(pageNumber);
        }

        [DoesNotReturn]
        private void ThrowObjectDisposed()
        {
            ThrowIf<ObjectDisposedException>(IsDisposed, "Transaction is already disposed");
            ThrowIf<InvalidDataException>(HasErrors, "The transaction is in error state, and cannot be used further");
            Throw<ObjectDisposedException>($"Transaction state is invalid: {_txStatus}");
        }

        public Page AllocatePage(int numberOfPages, long? pageNumber = null, Page? previousPage = null, bool zeroPage = true)
        {
            pageNumber ??= GetPageNumberForAllocation(numberOfPages);
            return AllocatePageImpl(numberOfPages, pageNumber.Value, previousPage, zeroPage);
        }

        public Page AllocateMultiplePageAndReturnFirst(int numberOfPages, bool zeroPage = true)
        {
            var firstPageNumber = GetPageNumberForAllocation(numberOfPages);
            var first = AllocatePageImpl(1, firstPageNumber, previousVersion: null, zeroPage);
            for (int i = 1; i < numberOfPages; i++)
            {
                AllocatePageImpl(1, firstPageNumber + i, previousVersion: null, zeroPage);
            }
            return first;
        }

        private long GetPageNumberForAllocation(int numberOfPages)
        {
            EnsureNotCurrentlyHoldingRootObjectsOpen();
            var pageNumber = _freeSpaceHandling.TryAllocateFromFreeSpace(this, numberOfPages);
            if (pageNumber != null)
                return pageNumber.Value;

            // allocate from end of file
            var eof = GetNextPageNumber();
            _localTxNextPageNumber += numberOfPages;
            return eof;
        }

        public long GetNextPageNumber() => _envRecord.NextPageNumber + _localTxNextPageNumber;

        public void SetNextPageNumber(long nextPageNumber) => _localTxNextPageNumber = nextPageNumber - _envRecord.NextPageNumber;

        [Conditional("DEBUG")]
        private void EnsureNotCurrentlyHoldingRootObjectsOpen()
        {
            if (RootObjects == null)
                return;
            using (new Tree.DirectAddScope(RootObjects))
            {
                // this ensures that we'll get consistent errors for RavenDB-20647
            }
        }

        public Page AllocateOverflowRawPage(long overflowSize, out int numberOfPages, long? pageNumber = null, Page? previousPage = null, bool zeroPage = true)
        {
            if (overflowSize > int.MaxValue - 1)
                throw new InvalidOperationException($"Cannot allocate chunks bigger than {int.MaxValue / 1024 * 1024} Mb.");

            Debug.Assert(overflowSize >= 0);

            numberOfPages = Paging.Paging.GetNumberOfOverflowPages(overflowSize);

            var overflowPage = AllocatePage(numberOfPages, pageNumber, previousPage, zeroPage);
            overflowPage.Flags = PageFlags.Overflow;
            overflowPage.OverflowSize = (int)overflowSize;

            return overflowPage;
        }

        private Page AllocatePageImpl(int numberOfPages, long pageNumber, Page? previousVersion, bool zeroPage)
        {
            if (IsValid == false)
                ThrowObjectDisposed();

            try
            {
                var maxAvailablePageNumber = _env.Options.MaxStorageSize / Constants.Storage.PageSize;

                if (pageNumber > maxAvailablePageNumber)
                    ThrowQuotaExceededException(pageNumber, maxAvailablePageNumber);


                Debug.Assert(pageNumber < GetNextPageNumber());

#if VALIDATE
            VerifyNoDuplicateScratchPages();
#endif
                var pageFromScratchBuffer = _env.ScratchBufferPool.Allocate(this, numberOfPages, pageNumber, previousVersion ?? default);


                AddTransactionPage(pageFromScratchBuffer);

                _numberOfModifiedPages += numberOfPages;

                _scratchPagesInUse.Set(pageNumber, pageFromScratchBuffer);

                _dirtyPages.Add(pageNumber);

                TrackDirtyPage(pageNumber);

                var page = pageFromScratchBuffer.ReadNewPage(this);
                if (zeroPage)
                    Memory.Set(page.Pointer, 0, Constants.Storage.PageSize * numberOfPages);
                var newPage = page with
                {
                    Flags = PageFlags.Single,
                    PageNumber = pageNumber
                };
                _pageLocator.SetWritable(newPage);

                TrackWritablePage(newPage);

#if VALIDATE
                VerifyNoDuplicateScratchPages();
#endif

                return newPage;
            }
            catch
            {
                _txStatus |= TxStatus.Errored;
                throw;
            }
        }

        [DoesNotReturn]
        private void ThrowQuotaExceededException(long pageNumber, long? maxAvailablePageNumber)
        {
            throw new QuotaException(
                $"The maximum storage size quota ({_env.Options.MaxStorageSize} bytes) has been reached. " +
                $"Currently configured storage quota is allowing to allocate the following maximum page number {maxAvailablePageNumber}, while the requested page number is {pageNumber}. " +
                "To increase the quota, use the MaxStorageSize property on the storage environment options.");
        }

        internal void ShrinkOverflowPage(long pageNumber, int newSize, Tree tree)
        {
            if (IsValid == false)
                ThrowObjectDisposed();

            if (_scratchPagesInUse.TryGetValue(pageNumber, out PageFromScratchBuffer value) == false)
                throw new InvalidOperationException($"The page {pageNumber} was not previous allocated in this transaction");

            // shrinking a page of a committed transaction would mutate its scratch memory in place (readers may
            // be looking at it) and would put a foreign page into _transactionPages, which both the journal write
            // and Rollback() treat as this transaction's own allocations
            Debug.Assert(value.AllocatedInTransaction == Id, $"ShrinkOverflowPage({pageNumber}) on a page allocated in tx {value.AllocatedInTransaction}, current tx is {Id}");

            var page = value.ReadWritable(this);
            if (page.IsOverflow == false || page.OverflowSize < newSize)
                throw new InvalidOperationException($"The page {pageNumber} was is not an overflow page greater than {newSize}");

            var prevNumberOfPages = Paging.Paging.GetNumberOfOverflowPages(page.OverflowSize);
            page.OverflowSize = newSize;
            var lowerNumberOfPages = Paging.Paging.GetNumberOfOverflowPages(newSize);

            Debug.Assert(lowerNumberOfPages != 0);

            if (prevNumberOfPages == lowerNumberOfPages)
                return;

            for (int i = lowerNumberOfPages; i < prevNumberOfPages; i++)
            {
                FreePage(page.PageNumber + i
#if DEBUG
                    , isOverflowShrink: true
#endif
                );
            }

            // need to set the proper number of pages in the scratch page

            var shrinked = value.File.ShrinkOverflowPage(value, lowerNumberOfPages);

            _scratchPagesInUse.Set(pageNumber, shrinked);
            ReplaceTransactionPage(value, shrinked);

            ref var treeMutableState = ref tree.ModifyHeader();
            treeMutableState.OverflowPages -= prevNumberOfPages - lowerNumberOfPages;
        }

        [Conditional("DEBUG")]
        public void VerifyNoDuplicateScratchPages()
        {
            var pageNums = new HashSet<long>();
            foreach (ref readonly var txPage in CollectionsMarshal.AsSpan(_transactionPages))
            {
                var scratchPage = txPage.ReadPage(this);
                if (pageNums.Add(scratchPage.PageNumber) == false)
                    throw new InvalidDataException($"Duplicate page in transaction: {scratchPage.PageNumber}");
            }
        }


        public bool IsValid => _txStatus == TxStatus.None;

        public bool IsDisposed => _txStatus.HasFlag(TxStatus.Disposed);

        public bool HasErrors => _txStatus.HasFlag(TxStatus.Errored);

        public int CurrentTransactionIdHolder { get; set; }

        public PageLocator PageLocator => _pageLocator;


        private readonly UnguardedDisposableScope _disposableScope = new();

        internal void RegisterDisposable<T>(T disposable) where T : IDisposable
        {
            _disposableScope.EnsureDispose(disposable);
        }

        public string CallerName { get; set; }

        internal Func<LowLevelTransaction, bool> AppliedJournalStateAction;

        public void Dispose()
        {
            if (IsDisposed)
                return;

            EnsureDisposeOfWriteTxIsOnTheSameThreadThatCreatedIt();

            try
            {
                if (!Committed && !RolledBack && Flags == TransactionFlags.ReadWrite)
                    Rollback();

                _txStatus |= TxStatus.Disposed;

                FreePageLocator(PersistentContext, ref _pageLocator);
            }
            finally
            {
                // After the transaction is completed, we will move on into disposing all resources used by this
                // transaction. First the pagers and internal resources, then registered ones by external parties.
                _env.TransactionCompleted(this);

                _disposableScope.Dispose();

                if (_disposeAllocator)
                {
                    if (_env.Options.Encryption.IsEnabled)
                    {
                        _allocator.Wipe();
                    }
                    _allocator.Dispose();
                }

                PagerTransactionState.InvokeDispose(_env, ref DataPagerState, ref PagerTransactionState);
                OnDispose?.Invoke(this);

                _asyncCommitPreviousTransaction = null;

                _envRecord = null; // RavenDB-22972 - it prevents from having a possible reference cycle via EnvironmentStateRecord.ClientState
            }
        }

        internal void MarkTransactionAsFailed()
        {
            _txStatus |= TxStatus.Errored;
        }

        void INotifyAllocationFailure.OnAllocationFailure<TAllocator>(ByteStringContext<TAllocator> context)
        {
            MarkTransactionAsFailed();
        }

        internal void FreePageOnCommit(long pageNumber)
        {
            _pagesToFreeOnCommit.Push(pageNumber);
        }

        internal void DiscardScratchModificationOn(long pageNumber)
        {
            if (_scratchPagesInUse.Remove(pageNumber, out var scratchPage)
                && scratchPage.AllocatedInTransaction == Id)
            {
                RemoveTransactionPage(scratchPage);

                _env.ScratchBufferPool.FreeImmediately(this, scratchPage.File.Number, scratchPage.PositionInScratchBuffer);

                if (_env.Options.Encryption.IsEnabled)
                {
                    // need to mark buffers as invalid for commit
                    var encryptionBuffers = PagerTransactionState.ForCrypto![scratchPage.File.Pager];
                    encryptionBuffers[scratchPage.PositionInScratchBuffer].SkipOnTxCommit = true;
                }
            }

            _dirtyPages.Remove(pageNumber);

            UntrackDirtyPage(pageNumber);
        }

        public void FreePage(long pageNumber
#if DEBUG
            , bool isOverflowShrink = false
#endif
        )
        {
            if (IsValid == false)
                ThrowObjectDisposed();

            try
            {
#if DEBUG
                if (isOverflowShrink == false)
                    TrackOverflowPageRemoval(pageNumber);
#endif
                UntrackPage(pageNumber);
                Debug.Assert(pageNumber >= 0);

                _pageLocator.Reset(pageNumber); // Remove it from the page locator.

                _freeSpaceHandling.FreePage(this, pageNumber);

                _freedPages ??= new HashSet<long>();
                _freedPages.Add(pageNumber);
                
                _hasFreePages = true;

                DiscardScratchModificationOn(pageNumber);
            }
            catch
            {
                _txStatus |= TxStatus.Errored;
                throw;
            }
        }

        internal HashSet<long> GetFreedPages()
        {
            return _freedPages;
        }

        private static readonly ObjectPool<CompactKey> _sharedCompactKeyPool = new(() => new CompactKey());

        public CompactKey AcquireCompactKey()
        {
            var key = _sharedCompactKeyPool.Allocate();
            key.Initialize(this);
            return key;
        }

        public struct CompactKeyScope(LowLevelTransaction _llt, CompactKey _key) : IDisposable
        {
            public void Dispose()
            {
                _llt.ReleaseCompactKey(ref _key);
            }
        }

        public CompactKeyScope AcquireCompactKey(out CompactKey key)
        {
            key = _sharedCompactKeyPool.Allocate();
            key.Initialize(this);
            return new CompactKeyScope(this, key);
        }

        public void ReleaseCompactKey(ref CompactKey key)
        {
            if (key == null)
                return;

            // The reason why we reset the key, which in turn will null the storage is to avoid cases of reused keys
            // been used by multiple operations. Eventually someone wil restore
            if (ReferenceEquals(key, CompactKey.NullInstance) == false)
            {
                key.Reset();
                _sharedCompactKeyPool.Free(key);
            }
            key = null;
        }

        internal void PrepareForCommit()
        {
            _root.PrepareForCommit();
        }

        public void Commit()
        {
            ValidateOverflowPagesRemoval();

            if (Flags != TransactionFlags.ReadWrite)
                return;// nothing to do

            WaitForPreviousTxToBeginJournalWrite();

            CommitStage1_CompleteTransaction();

            Debug.Assert(_writeToJournalState is not WriteToJournalState.None, "_writeToJournalState is not WriteToJournalState.None");

            if (_writeToJournalState is WriteToJournalState.ModifiedPages or WriteToJournalState.BranchCommits)
            {
                if (_updateLastWorkTime)
                {
                    Environment.LastWorkTime = DateTime.UtcNow;
                }
                
                CommitStage2_WriteToJournal(waitForDurability: true);
            }

            BeforeCommitFinalization?.Invoke(this);
            CommitStage3_DisposeTransactionResources();
        }

        // This task completes when the journal write is submitted, but not necessarily durable yet. Next tx can start, but this tx isn't published yet.
        internal Task<bool> AsyncCommit;
        // This task completes when the journal write for this transaction is durably stored on disk.
        internal Task DurableCommit;
        internal TaskCompletionSource PreparedDurableCommit;

        internal void AcknowledgeDurableCommit()
        {
            _env.DurableCommitAcknowledged?.Invoke();
        }

        internal void CompleteDurableCommit()
        {
            _env.MarkJournalWriteDurable(Id);
            PreparedDurableCommit?.TrySetResult();
            AcknowledgeDurableCommit();
        }

        
        internal void FailDurableCommit(Exception e)
        {
            try
            {   // a durable-commit failure is catastrophic by definition
                _env.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
            }
            catch{ /* best effort  */ }

            PreparedDurableCommit?.TrySetException(e);
            AcknowledgeDurableCommit();
        }

        internal void CancelDurableCommit()
        {
            PreparedDurableCommit?.TrySetCanceled();
            AcknowledgeDurableCommit();
        }
        
        private LowLevelTransaction _asyncCommitNextTransaction;
        private LowLevelTransaction _asyncCommitPreviousTransaction;
        private bool _asyncCommitSubmissionEnded;
        private static readonly Task<bool> NoWriteToJournalRequiredTask = Task.FromResult(false);

        /// <summary>
        /// This begins an async commit and starts a new transaction immediately.
        /// The current transaction is considered completed in memory but not yet
        /// committed to disk. This *must* be completed by calling EndAsyncCommit.
        /// </summary>
        public LowLevelTransaction BeginAsyncCommitAndStartNewTransaction(TransactionPersistentContext persistentContext)
        {
            if (Flags != TransactionFlags.ReadWrite)
                ThrowReadTransactionCannotDoAsyncCommit();
            if (_asyncCommitNextTransaction != null)
                ThrowAsyncCommitAlreadyCalled();

            WaitForPreviousTxToBeginJournalWrite();

            CommitStage1_CompleteTransaction();

            Debug.Assert(_writeToJournalState is not WriteToJournalState.None, "_writeToJournalState is not WriteToJournalState.None");
            bool writeToJournalIsRequired = _writeToJournalState is not WriteToJournalState.Skip;
            bool writesOwnJournalRecord = _writeToJournalState is WriteToJournalState.ModifiedPages;

            FreePageLocator(persistentContext, ref _pageLocator);

            var nextTx = new LowLevelTransaction(this, persistentContext,
                writesOwnJournalRecord ? Id + 1 : Id
            );
            _asyncCommitNextTransaction = nextTx;
            nextTx._asyncCommitPreviousTransaction = this;
            AsyncCommit = writeToJournalIsRequired
                  ? Task.Run(() => { CommitStage2_WriteToJournal(waitForDurability: false); return true; })
                  : NoWriteToJournalRequiredTask;

            var usageIncremented = false;

            try
            {
                _forTestingPurposes?.ActionToCallDuringBeginAsyncCommitAndStartNewTransaction?.Invoke();

                _env.IncrementUsageOnNewTransaction();
                usageIncremented = true;

                _env.ActiveTransactions.Add(nextTx);
                _env.WriteTransactionStarted();

                _env.InvokeNewTransactionCreated(nextTx);

                return nextTx;
            }
            catch (Exception)
            {
                // failure here means that we'll try to complete the current transaction normally
                // then throw as if commit was called normally and the next transaction failed

                try
                {
                    if (usageIncremented)
                        _env.DecrementUsageOnTransactionCreationFailure();

                    EndAsyncCommit();
                }
                finally
                {
                    AsyncCommit = null;
                }

                _txStatus |= TxStatus.Errored;

                throw;
            }
        }

        [DoesNotReturn]
        private static void ThrowAsyncCommitAlreadyCalled()
        {
            throw new InvalidOperationException("Cannot start a new async commit because one was already started");
        }

        [DoesNotReturn]
        private static void ThrowReadTransactionCannotDoAsyncCommit()
        {
            throw new InvalidOperationException("Only write transactions can do async commit");
        }

        private void WaitForPreviousTxToBeginJournalWrite()
        {
            var producer = _asyncCommitPreviousTransaction;
            _asyncCommitPreviousTransaction = null;

            // if we had a previous tx, we need to wait for it to build its journal entry
            // and start writing it. This is so we know the size, of the last written journal
            // position, so we know where *our* write goes (parallel journal writes)

            if (producer?.AsyncCommit != null)
                producer.EndAsyncCommitSubmission();
        }

        private void EndAsyncCommitSubmission()
        {
            if (AsyncCommit == null)
            {
                _txStatus |= TxStatus.Errored;
                ThrowInvalidAsyncEndWithoutBegin();
                return;// never reached
            }

            if (_asyncCommitSubmissionEnded)
                return;

            _asyncCommitSubmissionEnded = true;

            try
            {
                AsyncCommit.Wait();
            }
            catch (Exception e)
            {
                // an exception being thrown while / after / somewhere in the middle 
                // of writing to the journal means that we don't know what the current
                // state of the journal is. We have to shut down and run recovery to 
                // come to a known good state
                _txStatus |= TxStatus.Errored;
                // stage2 may have died before reaching the journal - nothing else will complete the window task
                FailDurableCommit(e);

                throw;
            }

            Debug.Assert(_asyncCommitNextTransaction.Committed == false, "_asyncCommitNextTransaction.Committed == false");
            // we need to update the state of the file position in the journal file, which happens in stage2 (async)
            // before we can actually commit the current transaction
            _asyncCommitNextTransaction.UpdateJournal(CurrentStateRecord.Journal.Number, CurrentStateRecord.Journal.Last4KWritePosition);
        }

        public void EndAsyncCommit()
        {
            EndAsyncCommitSubmission();

            if (AsyncCommit.Result)
            {
                if (_updateLastWorkTime)
                    Environment.LastWorkTime = DateTime.UtcNow;
            }

            try
            {
                _env.WaitForCommitDurability(CommitDurabilityGateTransactionId);
            }
            catch (Exception e)
            {
                _txStatus |= TxStatus.Errored;
                _env.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));

                throw;
            }

            BeforeCommitFinalization?.Invoke(this);
            CommitStage3_DisposeTransactionResources();
        }

        [DoesNotReturn]
        private static void ThrowInvalidAsyncEndWithoutBegin()
        {
            throw new InvalidOperationException("Cannot call EndAsyncCommit when we don't have an async op running");
        }

        private enum WriteToJournalState
        {
            None,
            Skip,
            ModifiedPages,
            BranchCommits
        }

        private WriteToJournalState _writeToJournalState;
        public bool ShouldWriteTransactionChangesToJournal
        {
            get
            {
                Debug.Assert(_writeToJournalState is not WriteToJournalState.None);
                return _writeToJournalState is WriteToJournalState.ModifiedPages;
            }
        }

        internal bool IsAsyncCommit => _asyncCommitNextTransaction != null;

        internal long CommitDurabilityGateTransactionId =>
            WrittenToJournalNumber == -1 ? CurrentStateRecord.TransactionId - 1 : CurrentStateRecord.TransactionId;

        private void CommitStage2_WriteToJournal(bool waitForDurability)
        {
            try
            {
                (long numberOfUncompressedPages, long numberOf4Kbs) = _journal.WriteToJournal(this);

                if (_forTestingPurposes?.SimulateThrowingOnCommitStage2 == true)
                    _forTestingPurposes.ThrowSimulateErrorOnCommitStage2();

                if (waitForDurability)
                    _env.WaitForCommitDurability(CommitDurabilityGateTransactionId);

                if (_requestedCommitStats == null)
                    return;

                _requestedCommitStats.NumberOfModifiedPages = numberOfUncompressedPages;
                _requestedCommitStats.NumberOf4KbsWrittenToDisk = numberOf4Kbs;
            }
            catch
            {
                _txStatus |= TxStatus.Errored;
                throw;
            }
        }

        private void CommitStage1_CompleteTransaction()
        {
            if (IsValid == false)
                ThrowObjectDisposed();

            if (Committed)
                ThrowAlreadyCommitted();

            if (RolledBack)
                ThrowAlreadyRolledBack();

            while (_pagesToFreeOnCommit.Count > 0)
            {
                FreePage(_pagesToFreeOnCommit.Pop());
            }

            if (GetNextPageNumber() <= 1)
                ThrowNextPageNumberCannotBeSmallerOrEqualThanOne();

            _txHeader.LastPageNumber = GetNextPageNumber() - 1;

            ref readonly var rootHeader = ref _root.ReadHeader();
            _txHeader.Root = rootHeader;
            _txHeader.TxMarker |= TransactionMarker.Commit;

            List<(long Start, long Count)> regions = null;
            if (_sparsePageRanges != null && _sparsePageRanges.Count != 0)
            {
                regions = _freeSpaceHandling.GetSparseRegions(this, _sparsePageRanges);
            }
            _envRecord = _envRecord with
            {
                Root = rootHeader,
                SparseRegions = regions
            };

            LastChanceToReadFromWriteTransactionBeforeCommit?.Invoke(this);

            _env.Journal.Applicator.OnTransactionCommitted(this);

            if (_dirtyPages.Count > 0 || _hasFreePages)
            {
                _writeToJournalState = WriteToJournalState.ModifiedPages;
                // all consumers of this task block on it (branch commits, the merger wait handle), so
                // synchronous completion releases them directly instead of hopping through the pool
#pragma warning disable RDB0008
                PreparedDurableCommit = new TaskCompletionSource();
#pragma warning restore RDB0008
                DurableCommit = PreparedDurableCommit.Task;
            }
            else if(_journal.HasBranchCommits)
                _writeToJournalState = WriteToJournalState.BranchCommits;
            else
                _writeToJournalState = WriteToJournalState.Skip;

            ScratchTableSnapshot = _scratchPagesInUse.CaptureSnapshot();
        }


        [DoesNotReturn]
        private static void ThrowNextPageNumberCannotBeSmallerOrEqualThanOne([CallerMemberName] string caller = null)
        {
            throw new InvalidOperationException($"{nameof(_envRecord.NextPageNumber)} cannot be <= 1 on {caller}.");
        }

        private void CommitStage3_DisposeTransactionResources()
        {
            // an exception being thrown after the transaction has been committed to disk 
            // will corrupt the in memory state, and require us to restart (and recover) to 
            // be in a valid state
            try
            {
                ValidateAllPages();

                PagerTransactionState.InvokeBeforeCommitFinalization(_env, ref DataPagerState, ref PagerTransactionState);

                Committed = true;

                _env.TransactionAfterCommit(this);
            }
            catch (Exception e)
            {
                _txStatus |= TxStatus.Errored;
                _env.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                throw;
            }
        }

        [DoesNotReturn]
        private static void ThrowAlreadyRolledBack()
        {
            throw new InvalidOperationException("Cannot commit rolled-back transaction.");
        }

        [DoesNotReturn]
        private static void ThrowAlreadyCommitted()
        {
            throw new InvalidOperationException("Cannot commit already committed transaction.");
        }

        [Conditional("DEBUG")]
        private void EnsureDisposeOfWriteTxIsOnTheSameThreadThatCreatedIt()
        {
            var threadStats = NativeMemory.GetByThreadId(System.Environment.CurrentManagedThreadId);
            if (Flags == TransactionFlags.ReadWrite && System.Environment.CurrentManagedThreadId != CurrentTransactionIdHolder)
            {
                throw new InvalidOperationException($"Dispose of the write transaction must be called from the same thread that created it. " +
                                                    $"Transaction {Id} (Flags: {Flags}) was created by '{threadStats?.Name}', thread Id: {CurrentTransactionIdHolder}. " +
                                                    $"The dispose was called from {NativeMemory.CurrentThreadStats.Name}, thread Id: {NativeMemory.CurrentThreadStats.ManagedThreadId}. " +
                                                    $"Do you have any await call in the scope of the write transaction?");
            }
        }

        public void Rollback()
        {
            // here we allow rolling back of errored transaction
            if (IsDisposed)
                ThrowObjectDisposed();

            if (Committed || RolledBack || Flags != (TransactionFlags.ReadWrite))
                return;

            OnRollBack?.Invoke(this);

            _freeSpaceHandling.OnRollback();

            ValidateReadOnlyPages();

            // we need to roll back all the changes we made here
            _scratchPagesInUse.RollbackCurrentTransaction();

            // We need to free pages allocated by this transaction in a scratch buffer.
            // During tx, we did partial cleanup via `DiscardScratchModificationOn`
            // So only pages currently allocated by this transaction need to be freed.
            var forTestingPurposes = _forTestingPurposes;
            foreach (ref readonly var scratchPage in CollectionsMarshal.AsSpan(_transactionPages))
            {
                if (forTestingPurposes != null)
                    forTestingPurposes.ScratchPagesExaminedDuringRollback++;

                Debug.Assert(scratchPage.AllocatedInTransaction == Id,
                    $"Scratch page {scratchPage.PositionInScratchBuffer} in the pages of transaction {Id} was allocated in transaction {scratchPage.AllocatedInTransaction}");

                _env.ScratchBufferPool.FreeImmediately(this, scratchPage.File.Number, scratchPage.PositionInScratchBuffer);
            }

            RolledBack = true;
        }

        public void RetrieveCommitStats(out CommitStats stats)
        {
            _requestedCommitStats = stats = new CommitStats();
        }

        public string GetTxState()
        {
            return _txStatus.ToString();
        }

        private EnvironmentStateRecord _envRecord;
        private long _localTxNextPageNumber;
        public DateTime TxStartTime;
        public bool IsCloned;
        internal long? LocalPossibleOldestReadTransaction;
        internal RacyConcurrentBag.Node ActiveTransactionNode;
        public Transaction Transaction;

        /// <summary>
        /// This prevents updating last work time even if the transaction is committed. Useful for updating internal data which is not
        /// part of work ordered by a user.
        /// </summary>
        public void DisableLastWorkTimeUpdate()
        {
            _updateLastWorkTime = false;
        }

#if DEBUG

        //overflowPageId, Parent
        private readonly Dictionary<long, long> _overflowPagesToBeRemoved = new();

        [Conditional("DEBUG")]
        private void ValidateOverflowPagesRemoval()
        {
            if (_overflowPagesToBeRemoved.Count == 0)
                return;

            StringBuilder sb = new();
            var pagesMissed = from overflowPage in _overflowPagesToBeRemoved
                              group overflowPage.Key by overflowPage.Value
                into g
                              select new
                              {
                                  Parent = g.Key,
                                  OverflowPages = g.Select(i => i).ToArray()
                              };

            sb.AppendLine("We commit but we still have not deleted all overflow pages to retrieve used storage. Missed pages: ");
            foreach (var pageMissed in pagesMissed)
            {
                sb.AppendLine($"Parent: {pageMissed.Parent}, Overflow pages: {string.Join(",", pageMissed.OverflowPages)}");
            }

            VoronUnrecoverableErrorException.Raise(_env, sb.ToString());
        }

        [Conditional("DEBUG")]
        private void TrackOverflowPageRemoval(long pageId)
        {
            if (_overflowPagesToBeRemoved.ContainsKey(pageId))
            {
                _overflowPagesToBeRemoved.Remove(pageId);
                return;
            }

            if (_envRecord.Root.PageCount < pageId)
                return;

            var page = GetPage(pageId);
            if (page.IsOverflow == false)
                return;

            int numberOfOverflowPages = Paging.Paging.GetNumberOfOverflowPages(page.OverflowSize);
            for (int pageOffset = 1; pageOffset < numberOfOverflowPages; ++pageOffset)
            {
                _overflowPagesToBeRemoved.Add(pageId + pageOffset, pageId);
            }
        }

#else
        [Conditional("DEBUG")]
        private void ValidateOverflowPagesRemoval(){}

        [Conditional("DEBUG")]
        private void TrackOverflowPageRemoval(long pageId){}

#endif


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
                
                ulong pageHash = StorageEnvironment.CalculatePageChecksum(page.Pointer, page.PageNumber, page.Flags, page.OverflowSize);
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
                ulong pageHash = StorageEnvironment.CalculatePageChecksum(page.Pointer, page.PageNumber, page.Flags, page.OverflowSize);
                writablePages[page.PageNumber] = pageHash;
            }
        }

        private void TrackReadOnlyPage(Page page)
        {
            if (writablePages.ContainsKey(page.PageNumber))
                return;

            ulong pageHash = StorageEnvironment.CalculatePageChecksum(page.Pointer, page.PageNumber, page.Flags, page.OverflowSize);

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

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff(this);
        }

        internal sealed class TestingStuff
        {
            private readonly LowLevelTransaction _tx;
            internal bool SimulateThrowingOnCommitStage2 = false;

            internal long ScratchPagesExaminedDuringRollback;
            internal Action ActionToCallDuringEnsurePagerStateReference;
            internal Action ActionToCallJustBeforeWritingToJournal;
            internal Action ActionToCallDuringBeginAsyncCommitAndStartNewTransaction;
            internal Action ActionToCallOnTransactionAfterCommit;

            public TestingStuff(LowLevelTransaction tx)
            {
                _tx = tx;
            }

            internal void SetLocalTxNextPageNumber(long n)
            {
                _tx._localTxNextPageNumber = n;
            }
            [DoesNotReturn]
            internal void ThrowSimulateErrorOnCommitStage2()
            {
                throw new InvalidOperationException("Simulation error");
            }

            internal IDisposable CallDuringEnsurePagerStateReference(Action action)
            {
                ActionToCallDuringEnsurePagerStateReference = action;

                return new DisposableAction(() => ActionToCallDuringEnsurePagerStateReference = null);
            }

            internal IDisposable CallJustBeforeWritingToJournal(Action action)
            {
                ActionToCallJustBeforeWritingToJournal = action;

                return new DisposableAction(() => ActionToCallJustBeforeWritingToJournal = null);
            }

            internal IDisposable CallDuringBeginAsyncCommitAndStartNewTransaction(Action action)
            {
                ActionToCallDuringBeginAsyncCommitAndStartNewTransaction = action;

                return new DisposableAction(() => ActionToCallDuringBeginAsyncCommitAndStartNewTransaction = null);
            }

            internal IDisposable CallOnTransactionAfterCommit(Action action)
            {
                ActionToCallOnTransactionAfterCommit = action;

                return new DisposableAction(() => ActionToCallOnTransactionAfterCommit = null);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal PersistentDictionary GetEncodingDictionary(long dictionaryId)
        {
            var dictionaryLocator = _env.DictionaryLocator;
            if (dictionaryLocator.TryGet(dictionaryId, out var dictionary))
                return dictionary;

            return _env.CreateEncodingDictionary(GetPage(dictionaryId));
        }

        public ByteStringContext<ByteStringMemoryCache>.InternalScope GetTempPage(int pageSize, out TreePage page)
        {
            var dispose = Allocator.Allocate(pageSize, out ByteString tmp);
            // Callers are fine with getting "dirty" data, but will actually make compressing for journal better
            tmp.Clear();
            TreePage.Initialize(tmp.Ptr, pageSize);
            page = new TreePage(tmp.Ptr, pageSize);
            return dispose;
        }

        public bool IsDirty(long p)
        {
            return _dirtyPages.Contains(p);
        }


        public void ForgetAboutScratchPage(in PageFromScratchBuffer value)
        {
            if (_scratchPagesInUse.TryGetValue(value.PageNumberInDataFile, out var existing) == false)
            {
                // page may have been freed, that is expected
                return;
            }

            Debug.Assert(value.PageNumberInDataFile == existing.PageNumberInDataFile);
            if (value.AllocatedInTransaction != existing.AllocatedInTransaction)
                return; // transaction scratch page is different

            _scratchPagesInUse.RemoveFlushed(value.PageNumberInDataFile);
        }

        public void RecordSparseRangeCandidate(long sectionPageNumber)
        {
            _sparsePageRanges ??= new();
            _sparsePageRanges.Add(sectionPageNumber);
        }
    }
}
