using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Utils;
using Voron.Data;
using Voron.Exceptions;
using Voron.Global;

namespace Voron.Impl.Paging
{
    public abstract unsafe class AbstractPager : IDisposable
    {
        private readonly StorageEnvironmentOptions _options;

        public static ConcurrentDictionary<string, uint> PhysicalDrivePerMountCache = new ConcurrentDictionary<string, uint>();

        protected int MinIncreaseSize => 16 * Constants.Size.Kilobyte;

        protected int MaxIncreaseSize => Constants.Size.Gigabyte;

        private long _increaseSize;
        private DateTime _lastIncrease;
        private readonly object _pagerStateModificationLocker = new object();
        public bool UsePageProtection { get; } = false;

        public void SetPagerState(PagerState newState)
        {
            if (Disposed)
                ThrowAlreadyDisposedException();

            lock (_pagerStateModificationLocker)
            {
                _debugInfo = GetSourceName();
                var oldState = _pagerState;
                newState.AddRef();
                _pagerState = newState;
                oldState?.Release();
            }
        }

        internal PagerState GetPagerStateAndAddRefAtomically()
        {
            if (Disposed)
                ThrowAlreadyDisposedException();

            lock (_pagerStateModificationLocker)
            {
                if (_pagerState == null)
                    return null;
                _pagerState.AddRef();
                return _pagerState;
            }
        }

        public PagerState PagerState
        {
            get
            {
                if (Disposed)
                    ThrowAlreadyDisposedException();
                return _pagerState;
            }
        }

        private string _debugInfo;

        public string DebugInfo
        {
            get { return _debugInfo; }
        }

        public const int PageMaxSpace = Constants.Storage.PageSize - Constants.Tree.PageHeaderSize;

        public string FileName;

        protected AbstractPager(StorageEnvironmentOptions options, bool usePageProtection = false)
        {
            _options = options;
            UsePageProtection = usePageProtection;
            Debug.Assert((Constants.Storage.PageSize - Constants.Tree.PageHeaderSize) / Constants.Tree.MinKeysInPage >= 1024);


            NodeMaxSize = PageMaxSpace / 2 - 1;

            // MaxNodeSize is usually persisted as an unsigned short. Therefore, we must ensure it is not possible to have an overflow.
            Debug.Assert(NodeMaxSize < ushort.MaxValue);

            _increaseSize = MinIncreaseSize;

            PageMinSpace = (int)(PageMaxSpace * 0.33);

            SetPagerState(new PagerState(this));
        }

        public StorageEnvironmentOptions Options => _options;

        public int PageMinSpace
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set;
        }

        public bool DeleteOnClose
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set;
        }

        public int NodeMaxSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set;
        }

        public const int RequiredSpaceForNewNode = Constants.Tree.NodeHeaderSize + Constants.Tree.NodeOffsetSize;


        private PagerState _pagerState;

        public long NumberOfAllocatedPages { get; protected set; }
        public abstract long TotalAllocationSize { get; }

        protected abstract string GetSourceName();

        public virtual byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            if (Disposed)
                ThrowAlreadyDisposedException();

            if (pageNumber > NumberOfAllocatedPages || pageNumber < 0)
                ThrowOnInvalidPageNumber(pageNumber, tx?.Environment);

            var state = pagerState ?? _pagerState;

            tx?.EnsurePagerStateReference(state);

            return state.MapBase + pageNumber * Constants.Storage.PageSize;
        }

        public abstract void Sync();


        public PagerState EnsureContinuous(long requestedPageNumber, int numberOfPages)
        {
            if (Disposed)
                ThrowAlreadyDisposedException();

            if (requestedPageNumber + numberOfPages <= NumberOfAllocatedPages)
                return null;

            // this ensure that if we want to get a range that is more than the current expansion
            // we will increase as much as needed in one shot
            var minRequested = (requestedPageNumber + numberOfPages) * Constants.Storage.PageSize;
            var allocationSize = Math.Max(NumberOfAllocatedPages * Constants.Storage.PageSize, Constants.Storage.PageSize);
            while (minRequested > allocationSize)
            {
                allocationSize = GetNewLength(allocationSize);
            }

            return AllocateMorePages(allocationSize);
        }

        [Conditional("VALIDATE")]
        internal virtual void ProtectPageRange(byte* start, ulong size, bool force = false)
        {
            // This method is currently implemented only in Win32MemoryMapPager and POSIX
        }

        [Conditional("VALIDATE")]
        internal virtual void UnprotectPageRange(byte* start, ulong size, bool force = false)
        {
            // This method is currently implemented only in Win32MemoryMapPager and POSIX
        }

        public bool Disposed { get; private set; }

        public uint UniquePhysicalDriveId;

        public virtual void Dispose()
        {
            if (Disposed)
                return;

            _options.IoMetrics.FileClosed(FileName);

            if (_pagerState != null)
            {
                _pagerState.Release();
                _pagerState = null;
            }

            Disposed = true;
            NativeMemory.UnregisterFileMapping(FileName);
            GC.SuppressFinalize(this);
        }

        ~AbstractPager()
        {
            Dispose();
        }

        protected abstract PagerState AllocateMorePages(long newLength);


        private long GetNewLength(long current)
        {
            DateTime now = DateTime.UtcNow;
            if (_lastIncrease == DateTime.MinValue)
            {
                _lastIncrease = now;
                return MinIncreaseSize;
            }

            TimeSpan timeSinceLastIncrease = (now - _lastIncrease);
            if (timeSinceLastIncrease.TotalMinutes < 3)
            {
                _increaseSize = Math.Min(_increaseSize * 2, MaxIncreaseSize);
            }
            else if (timeSinceLastIncrease.TotalMinutes > 15)
            {
                _increaseSize = Math.Max(MinIncreaseSize, _increaseSize / 2);
            }

            _lastIncrease = now;
            // At any rate, we won't do an increase by over 50% of current size, to prevent huge empty spaces
            // 
            // The reasoning behind this is that we want to make sure that we increase in size very slowly at first
            // because users tend to be sensitive to a lot of "wasted" space. 
            // We also consider the fact that small increases in small files would probably result in cheaper costs, and as
            // the file size increases, we will reserve more & more from the OS.
            // This also plays avoids "I added 300 records and the file size is 64MB" problems that occur when we are too
            // eager to reserve space
            var actualIncrease = Math.Min(_increaseSize, current / 2);

            // we then want to get the next power of two number, to get pretty file size
            return current + Bits.NextPowerOf2(actualIncrease);
        }


        public abstract override string ToString();


        public static void ThrowAlreadyDisposedException()
        {
            // this is a separate method because we don't want to have an exception throwing in the hot path
            throw new ObjectDisposedException("The pager is already disposed");
        }


        protected void ThrowOnInvalidPageNumber(long pageNumber, StorageEnvironment env)
        {
            // this is a separate method because we don't want to have an exception throwing in the hot path

            VoronUnrecoverableErrorException.Raise(env,
                "The page " + pageNumber + " was not allocated, allocated pages: " + NumberOfAllocatedPages + " in " +
                GetSourceName());
        }

        public abstract void ReleaseAllocationInfo(byte* baseAddress, long size);

        // NodeMaxSize - RequiredSpaceForNewNode for 4Kb page is 2038, so we drop this by a bit
        public const int MaxKeySize = 2038 - RequiredSpaceForNewNode;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsKeySizeValid(int keySize)
        {
            if (keySize > MaxKeySize)
                return false;

            return true;
        }

        public abstract void TryPrefetchingWholeFile();
        public abstract void MaybePrefetchMemory(List<long> pagesToPrefetch);

        public virtual void EnsureMapped(IPagerLevelTransactionState tx, long page, int numberOfPages)
        {
            // nothing to do
        }

        public virtual int CopyPage(I4KbBatchWrites destwI4KbBatchWrites, long p, PagerState pagerState)
        {
            var src = AcquirePagePointer(null, p, pagerState);
            var pageHeader = (PageHeader*)src;
            int numberOfPages = 1;
            if ((pageHeader->Flags & PageFlags.Overflow) == PageFlags.Overflow)
            {
                numberOfPages = this.GetNumberOfOverflowPages(pageHeader->OverflowSize);
            }
            const int adjustPageSize = (Constants.Storage.PageSize)/(4*Constants.Size.Kilobyte);
            destwI4KbBatchWrites.Write(pageHeader->PageNumber * (long)adjustPageSize, numberOfPages * adjustPageSize, src);

            return numberOfPages;
        }

        public virtual I4KbBatchWrites BatchWriter()
        {
            return new Simple4KbBatchWrites(this);
        }
    }

    public interface I4KbBatchWrites : IDisposable
    {
        unsafe void Write(long posBy4Kbs, int numberOf4Kbs, byte* source);
    }

    public unsafe class Simple4KbBatchWrites : I4KbBatchWrites
    {
        private readonly AbstractPager _abstractPager;
        private PagerState _pagerState;

        public Simple4KbBatchWrites(AbstractPager abstractPager)
        {
            _abstractPager = abstractPager;
            _pagerState = _abstractPager.GetPagerStateAndAddRefAtomically();
        }

        public void Write(long posBy4Kbs, int numberOf4Kbs, byte* source)
        {
            const int pageSizeTo4KbRatio = (Constants.Storage.PageSize / (4 * Constants.Size.Kilobyte));
            var pageNumber = posBy4Kbs / pageSizeTo4KbRatio;
            var offsetBy4Kb = posBy4Kbs % pageSizeTo4KbRatio;
            var numberOfPages = numberOf4Kbs / pageSizeTo4KbRatio;
            if (numberOf4Kbs % pageSizeTo4KbRatio != 0)
                numberOfPages++;

            var newPagerState = _abstractPager.EnsureContinuous(pageNumber, numberOfPages);
            if (newPagerState != null)
            {
                _pagerState.Release();
                newPagerState.AddRef();
                _pagerState = newPagerState;
            }

            var toWrite = numberOf4Kbs * 4 * Constants.Size.Kilobyte;
            byte* destination = _abstractPager.AcquirePagePointer(null, pageNumber, _pagerState)
                                + (offsetBy4Kb * 4 * Constants.Size.Kilobyte);

            _abstractPager.UnprotectPageRange(destination, (ulong)toWrite);

            Memory.BulkCopy(destination, source, toWrite);

            _abstractPager.ProtectPageRange(destination, (ulong)toWrite);
        }

        public void Dispose()
        {
            _pagerState.Release();
        }
    }
}

