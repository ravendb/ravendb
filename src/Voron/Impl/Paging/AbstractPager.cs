using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Sparrow;
using Sparrow.Binary;
using Voron.Platform.Win32;
using System.Runtime.InteropServices;

namespace Voron.Impl.Paging
{
    public unsafe abstract class AbstractPager : IDisposable
    {
        protected int MinIncreaseSize { get { return 16 * _pageSize; } } // 64 KB with 4Kb pages. 
        protected int MaxIncreaseSize { get { return Constants.Size.Gigabyte; } }
        private static readonly IntPtr _currentProcess = Win32NativeMethods.GetCurrentProcess();

        private long _increaseSize;
        private DateTime _lastIncrease;
        protected readonly int _pageSize;

        public PagerState PagerState
        {
            get
            {
                if (Disposed)
                    ThrowAlreadyDisposedException();
                return _pagerState;
            }
            set
            {
                if (Disposed)
                    ThrowAlreadyDisposedException();

                _debugInfo = GetSourceName();
                _pagerState = value;
            }
        }

        private string _debugInfo;

        public string DebugInfo
        {
            get { return _debugInfo; }
        }

        protected AbstractPager(int pageSize)
        {
            Debug.Assert((pageSize - Constants.TreePageHeaderSize) / Constants.MinKeysInPage >= 1024);

            _pageSize = pageSize;

            PageMaxSpace = pageSize - Constants.TreePageHeaderSize;
            NodeMaxSize = PageMaxSpace / 2 - 1;

            // MaxNodeSize is usually persisted as an unsigned short. Therefore, we must ensure it is not possible to have an overflow.
            Debug.Assert(NodeMaxSize < ushort.MaxValue);
            
            _increaseSize = MinIncreaseSize;

            PageMinSpace = (int)(PageMaxSpace * 0.33);
            PagerState = new PagerState(this);
          
            PagerState.AddRef();
        }

        public int PageSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _pageSize; }
        }

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

        public int PageMaxSpace
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set;
        }

        public static readonly int RequiredSpaceForNewNode = Constants.NodeHeaderSize + Constants.NodeOffsetSize;


        private PagerState _pagerState;
        private readonly ConcurrentBag<Task> _tasks = new ConcurrentBag<Task>();

        public long NumberOfAllocatedPages { get; protected set; }

        protected abstract string GetSourceName();

        public virtual byte* AcquirePagePointer(LowLevelTransaction tx, long pageNumber, PagerState pagerState = null)
        {
            if (Disposed)
                ThrowAlreadyDisposedException();

            if (pageNumber > NumberOfAllocatedPages)
                ThrowOnInvalidPageNumber(pageNumber);

            var state = pagerState ?? _pagerState;

            tx?.EnsurePagerStateReference(state);

            return state.MapBase + pageNumber * _pageSize;
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
            var minRequested = (requestedPageNumber + numberOfPages) * _pageSize;
            var allocationSize = Math.Max(NumberOfAllocatedPages * _pageSize, PageSize);
            while (minRequested > allocationSize)
            {
                allocationSize = GetNewLength(allocationSize);
            }

            return AllocateMorePages(allocationSize);
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
            return current + Bits.NextPowerOf2(actualIncrease);
        }

        public int WriteDirect(byte* p, long pagePosition, int pagesToWrite)
        {
            if (Disposed)
                ThrowAlreadyDisposedException();

            int toCopy = pagesToWrite * _pageSize;
            Memory.BulkCopy(PagerState.MapBase + pagePosition * _pageSize, p, toCopy);

            return toCopy;
        }
        public override abstract string ToString();

        public void RegisterDisposal(Task run)
        {
            _tasks.Add(run);
        }

        public static void ThrowAlreadyDisposedException()
        {
            // this is a separate method because we don't want to have an exception throwing in the hot path
            throw new ObjectDisposedException("The pager is already disposed");
        }


        protected void ThrowOnInvalidPageNumber(long pageNumber)
        {
            // this is a separate method because we don't want to have an exception throwing in the hot path

            throw new ArgumentOutOfRangeException(nameof(pageNumber),
                "The page " + pageNumber + " was not allocated, allocated pages: " + NumberOfAllocatedPages + " in " +
                GetSourceName());
        }

        public abstract void ReleaseAllocationInfo(byte* baseAddress, long size);        

        // NodeMaxSize - RequiredSpaceForNewNode for 4Kb page is 2038, so we drop this by a bit
        public static readonly int MaxKeySize = 2038 - RequiredSpaceForNewNode;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsKeySizeValid(int keySize)
        {           
            if (keySize > MaxKeySize)
                return false;

            return true;
        }

        public void MaybePrefetchMemory(List<long> pagesToPrefetch)
        {
            if (Sparrow.Platform.Platform.CanPrefetch == false)
                return; // not supported

            if (pagesToPrefetch.Count == 0)
                return;

            var entries = new Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY[pagesToPrefetch.Count];
            for (int i = 0; i < entries.Length; i++)
            {
                entries[i].NumberOfBytes = (IntPtr)(4 * PageSize);
                entries[i].VirtualAddress = AcquirePagePointer(null, pagesToPrefetch[i]);
            }

            fixed (Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY* entriesPtr = entries)
            {
                Win32MemoryMapNativeMethods.PrefetchVirtualMemory(_currentProcess,
                    (UIntPtr)PagerState.AllocationInfos.Length, entriesPtr, 0);
            }
        }
    }
}

