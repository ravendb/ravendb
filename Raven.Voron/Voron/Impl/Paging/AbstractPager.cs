using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Sparrow;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Paging
{
    public unsafe abstract class AbstractPager : IVirtualPager
    {
        // Because this is known by the time the JIT would compile, it can be treated as a constant and act accordingly.
        public static readonly bool CanPrefetch = IsWindows8OrNewer();

        protected int MinIncreaseSize { get { return 16 * PageSize; } } // 64 KB
        protected int MaxIncreaseSize { get { return 262144 * PageSize; } } // 1 GB

        private long _increaseSize;
        private DateTime _lastIncrease;

        public PagerState PagerState
        {
            get
            {
                ThrowObjectDisposedIfNeeded();
                return _pagerState;
            }
            set
            {
                ThrowObjectDisposedIfNeeded();
                
                _source = GetSourceName();
                _pagerState = value;
            }
        }

        private string _source;
        protected AbstractPager()
        {
            _increaseSize = MinIncreaseSize;

            // MaxNodeSize is usually persisted as an unsigned short. Therefore, we must ensure it is not possible to have an overflow.
            Debug.Assert(NodeMaxSize < ushort.MaxValue);
            Debug.Assert((PageSize - Constants.PageHeaderSize) / Constants.MinKeysInPage >= 1024);
            
            PageMinSpace = (int)(PageMaxSpace * 0.33);
            PagerState = new PagerState(this);
          
            PagerState.AddRef();
        }

        public int PageMinSpace { get; private set; }

        public bool DeleteOnClose { get; set; }

        public const int PageSize = 4096;
        public readonly static int PageMaxSpace = PageSize - Constants.PageHeaderSize;

        public static readonly int RequiredSpaceForNewNode = Constants.NodeHeaderSize + Constants.NodeOffsetSize;
        public static readonly int RequiredSpaceForNewNodePrefixedKeys = Constants.NodeHeaderSize + Constants.NodeOffsetSize + Constants.PrefixedSliceHeaderSize;

        public readonly static int NodeMaxSize = PageMaxSpace / 2 - 1;
        public static readonly int NodeMaxSizePrefixedKeys = (PageMaxSpace - (Constants.PrefixInfoSectionSize + (Page.PrefixCount*Constants.PrefixNodeHeaderSize) + Page.PrefixCount /* possible 2-byte alignment for each prefix */))/2 - 1;

        private PagerState _pagerState;
        private readonly ConcurrentBag<Task> _tasks = new ConcurrentBag<Task>();

        public long NumberOfAllocatedPages { get; protected set; }

        public Page Read(Transaction tx, long pageNumber, PagerState pagerState = null)
        {
            ThrowObjectDisposedIfNeeded();
            
            if (pageNumber + 1 > NumberOfAllocatedPages)
            {
                throw new InvalidOperationException("Cannot get page number " + pageNumber +
                                                    " because number of allocated pages is " + NumberOfAllocatedPages);
            }

            return new Page(AcquirePagePointer(tx, pageNumber, pagerState), _source, PageSize);
        }

        protected abstract string GetSourceName();

        public virtual void TryPrefetchingWholeFile()
        {
            // do nothing
        }

        public virtual void MaybePrefetchMemory(List<Page> sortedPages)
        {
            // do nothing
        }

        public virtual void MaybePrefetchMemory(List<long> pages)
        {
            // do nothing
        }

        public virtual byte* AcquirePagePointer(Transaction tx, long pageNumber, PagerState pagerState = null)
        {
            ThrowObjectDisposedIfNeeded();

            var state = pagerState ?? PagerState;

            if (tx != null)
                tx.EnsurePagerStateReference(state);

            return state.MapBase + pageNumber * PageSize;
        }

        public abstract void Sync();

        public bool WillRequireExtension(long requestedPageNumber, int numberOfPages)
        {
            ThrowObjectDisposedIfNeeded();
            
            return requestedPageNumber + numberOfPages > NumberOfAllocatedPages;
        }

        public void EnsureContinuous(Transaction tx, long requestedPageNumber, int numberOfPages)
        {
            ThrowObjectDisposedIfNeeded();

            if (requestedPageNumber + numberOfPages <= NumberOfAllocatedPages)
                return;

            // this ensure that if we want to get a range that is more than the current expansion
            // we will increase as much as needed in one shot
            var minRequested = (requestedPageNumber + numberOfPages) * PageSize;
            var allocationSize = Math.Max(NumberOfAllocatedPages * PageSize, PageSize);
            while (minRequested > allocationSize)
            {
                allocationSize = GetNewLength(allocationSize);
            }

            AllocateMorePages(tx, allocationSize);

        }

        public bool ShouldGoToOverflowPage(int len)
        {
            ThrowObjectDisposedIfNeeded();

            return len + Constants.PageHeaderSize > NodeMaxSize;
        }

        public int GetNumberOfOverflowPages(int overflowSize)
        {
            ThrowObjectDisposedIfNeeded();

            overflowSize += Constants.PageHeaderSize;
            return (overflowSize / PageSize) + (overflowSize % PageSize == 0 ? 0 : 1);
        }


        public virtual int Write(Page page, long? pageNumber)
        {
            long startPage = pageNumber ?? page.PageNumber;

            int toWrite = page.IsOverflow ? GetNumberOfOverflowPages(page.OverflowSize) : 1;

            return WriteDirect(page, startPage, toWrite);
        }

        public bool Disposed { get; private set; }

        public virtual void Dispose()
        {
            if (Disposed)
                return;

            if (PagerState != null)
            {
                PagerState.Release();
                PagerState = null;
            }

            Task.WaitAll(_tasks.ToArray());

            Disposed = true;
            GC.SuppressFinalize(this);
        }

        ~AbstractPager()
        {
            Dispose();
        }

        public abstract void AllocateMorePages(Transaction tx, long newLength);

        private long GetNewLength(long current)
        {
            DateTime now = DateTime.UtcNow;
            if (_lastIncrease == DateTime.MinValue)
            {
                _lastIncrease = now;
                return MinIncreaseSize;
            }

            TimeSpan timeSinceLastIncrease = (now - _lastIncrease);
            if (timeSinceLastIncrease.TotalSeconds < 30)
            {
                _increaseSize = Math.Min(_increaseSize * 2, MaxIncreaseSize);
            }
            else if (timeSinceLastIncrease.TotalMinutes > 2)
            {
                _increaseSize = Math.Max(MinIncreaseSize, _increaseSize / 2);
            }

            _lastIncrease = now;
            // At any rate, we won't do an increase by over 25% of current size, to prevent huge empty spaces
            // 
            // The reasoning behind this is that we want to make sure that we increase in size very slowly at first
            // because users tend to be sensitive to a lot of "wasted" space. 
            // We also consider the fact that small increases in small files would probably result in cheaper costs, and as
            // the file size increases, we will reserve more & more from the OS.
            // This also plays avoids "I added 300 records and the file size is 64MB" problems that occur when we are too
            // eager to reserve space
            var actualIncrease = Math.Min(_increaseSize, current / 4);

            // we then want to get the next power of two number, to get pretty file size
            return current + Utils.NearestPowerOfTwo(actualIncrease);
        }

        public virtual int WriteDirect(Page start, long pagePosition, int pagesToWrite)
        {
            ThrowObjectDisposedIfNeeded();

            int toCopy = pagesToWrite * PageSize;
            Memory.BulkCopy(PagerState.MapBase + pagePosition * PageSize, start.Base, toCopy);

            return toCopy;
        }
        public override abstract string ToString();

        public void RegisterDisposal(Task run)
        {
            _tasks.Add(run);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void ThrowObjectDisposedIfNeeded()
        {
            if (Disposed)
                throw new ObjectDisposedException("The pager is already disposed");
        }

        public abstract void ReleaseAllocationInfo(byte* baseAddress, long size);

        public static int GetMaxKeySize(bool keysPrefixing)
        {
            return keysPrefixing == false 
                ? NodeMaxSize - RequiredSpaceForNewNode 
                : NodeMaxSizePrefixedKeys - RequiredSpaceForNewNodePrefixedKeys;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsKeySizeValid(int keySize, bool keysPrefixing)
        {
            if (keysPrefixing == false)
            {
                if (keySize + RequiredSpaceForNewNode > NodeMaxSize)
                    return false;

                return true;
            }

            if (keySize + RequiredSpaceForNewNodePrefixedKeys > NodeMaxSizePrefixedKeys)
                return false;

            return true;
        }


        private static bool IsWindows8OrNewer()
        {
            var os = Environment.OSVersion;
            return os.Platform == PlatformID.Win32NT &&
                   (os.Version.Major > 6 || (os.Version.Major == 6 && os.Version.Minor >= 2));
        }
    }
}
