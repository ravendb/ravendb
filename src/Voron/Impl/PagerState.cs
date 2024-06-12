using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Binary;
using Sparrow.Server.Platform;
using Voron.Global;
using Voron.Impl.Paging;


namespace Voron.Impl
{
    public sealed unsafe class PagerState
    {
        private readonly AbstractPager _pager;

        public bool DisposeFileOnDispose = true;

        public sealed class AllocationInfo
        {
            public MemoryMappedFile MappedFile;
            public byte* BaseAddress;
            public long Size;

            public override string ToString()
            {
                return $"{nameof(BaseAddress)}: {new IntPtr(BaseAddress):x} - {new IntPtr(BaseAddress + Size):x} {nameof(Size)}: {Size}";
            }
        }

#if DEBUG_PAGER_STATE
        public static ConcurrentDictionary<PagerState, string> Instances = new ConcurrentDictionary<PagerState, string>();
#endif

        public PagerState(AbstractPager pager, long prefetchSegmentSize, long prefetchResetThreshold, AllocationInfo allocationInfo = null)
        {
            _pager = pager;

#if DEBUG_PAGER_STATE
            Instances[this] = Environment.StackTrace;
#endif

            long sizeInBytes = 0;
            if (allocationInfo != null)
            {
                this.AllocationInfos = new[] { allocationInfo };
                this.MapBase = allocationInfo.BaseAddress;
                this.File = allocationInfo.MappedFile;
                sizeInBytes = allocationInfo.Size;
            }
            else
            {
                this.AllocationInfos = new AllocationInfo[] { };
                this.MapBase = null;
                this.File = null;
            }
                
            this._segmentShift = Bits.MostSignificantBit(prefetchSegmentSize);

            this._prefetchSegmentSize = 1 << this._segmentShift;
            this._prefetchResetThreshold = (int)((float)prefetchResetThreshold / this._prefetchSegmentSize);

            Debug.Assert((_prefetchSegmentSize - 1) >> this._segmentShift == 0);
            Debug.Assert(_prefetchSegmentSize >> this._segmentShift == 1);

            long numberOfAllocatedSegments = (sizeInBytes / _prefetchSegmentSize) + 1;
            this._prefetchTable = new byte[(numberOfAllocatedSegments / 2) + 1];
        }

        private const byte EvenPrefetchCountMask = 0x70;
        private const byte EvenPrefetchMaskShift = 4;
        private const byte OddPrefetchCountMask = 0x07;
        private const byte AlreadyPrefetch = 7;

        private readonly int _prefetchSegmentSize;
        private readonly int _prefetchResetThreshold;

        private readonly int _segmentShift;

        // this state is accessed by multiple threads
        // concurrently in an unsafe manner, we do so
        // explicitly with the intention of dealing with
        // dirty reads and writes. The only impact that this
        // can have is a spurious call to the OS's 
        // madvice() / PrefetchVirtualMemory
        // Thread safety is based on the OS's own thread safety
        // for concurrent calls to these methods. 
        private int _refreshCounter;
        private readonly byte[] _prefetchTable;

        private int _refs;

        public MemoryMappedFile File;

        public AllocationInfo[] AllocationInfos;

        public bool DiscardOnTxCopy;

        public readonly byte* MapBase; // { get; set; }

        public void Release()
        {
            int currentRefCount;
            int newRefCount;

            // Uses atomic compare-and-swap to safely decrement the reference count,
            // ensuring consistency even with concurrent modifications by other threads.
            // This loop ensures that the reference count is decremented only if it has not
            // been changed by another thread between reading and writing.
            do
            {
                currentRefCount = _refs;

                // Check for negative and zero reference counts to detect potential misuse or logical errors,
                // such as double releases or illegal state transitions, preventing further incorrect decrements.
                // This prevents the reference count from becoming negative.
                if (currentRefCount <= 0)
                    ThrowInvalidPagerState();

                newRefCount = currentRefCount - 1;
            }
            while (Interlocked.CompareExchange(ref _refs, newRefCount, currentRefCount) != currentRefCount);

            // Upon exiting the loop, the reference count is updated, and the sequence property
            // (v(n) != v(n+1)) is maintained, ensuring that no two consecutive operations 
            // can observe the same value for _refs due to the CAS guarantees.

            // When reaching this line, we know:
            // - _refs value was strictly positive before entering the compare and exchange loop
            // - _refs is either positive or zero after the loop
            // - _refs cannot be negative here because the loop bails with an exception if _refs is <= 0.

            // If the reference count is still positive, there are active references.
            if (newRefCount > 0)
                return;

            // Ensure reference count does not go negative, catching and preventing logical errors in reference management,
            // thereby maintaining integrity in debug builds. This is a safeguard to ensure the reference count
            // logic is correct and does not result in illegal states.
            EnsureNoNegativeReferenceCount(newRefCount);

            // Only proceed with cleanup if the CAS operation was successful, i.e., _refs was 0.
            if (Interlocked.CompareExchange(ref _refs, ReleasedReferenceCount, 0) != 0)
            {
                // If _refs was not 0, it means another thread already set it to ReleasedReferenceCount
                // or a new reference was added concurrently. Hence, no need to clean up yet.
                return;
            }

#if DEBUG_PAGER_STATE
            // In debug mode, removes the instance from tracking to help diagnose reference issues,
            // ensuring the debug state is consistent and does not grow unbounded.

            String value;
            Instances.TryRemove(this, out value);
#endif

            // The only way we can arrive here is when newRefCount is when we are ready to release. 
            // This ensures that ReleaseInternal is only called when no more references are held.
            ReleaseInternal();
        }

        [Conditional("DEBUG")]
        [Conditional("VALIDATE")]
        private void EnsureNoNegativeReferenceCount(int refCount)
        {
            // Validate that the reference count does not drop below zero,
            // catching potential logical errors and preventing them from affecting reference management.
            if (refCount < 0)
                throw new InvalidOperationException("The ref count is negative. This can't happen");
        }

        public void EnsurePageStateIsValid()
        {
            // Ensure that the page state is valid by checking if the reference count is <= 0,
            // which would indicate an invalid or already disposed state.
            if (_refs <= 0)
                ThrowInvalidPagerState();
        }

        public bool IsReleased
        {
            // Determine if the page is released by checking if the reference count is negative,
            // indicating that the page has been marked as released.
            get { return _refs < 0; }
        }

        private const int ReleasedReferenceCount = -1000000;
        
        private void ReleaseInternal()
        {
            // Release allocation info and dispose of the file if necessary,
            // ensuring proper cleanup of resources associated with this pager state.
            if (AllocationInfos != null)
            {
                foreach (var allocationInfo in AllocationInfos)
                    _pager.ReleaseAllocationInfo(allocationInfo.BaseAddress, allocationInfo.Size);
                AllocationInfos = null;            
            }

            if (File != null && DisposeFileOnDispose)
            {
                File.Dispose();
                File = null;
            }
        }

#if DEBUG_PAGER_STATE
        public ConcurrentQueue<string> AddedRefs = new ConcurrentQueue<string>();
#endif
        public void AddRef()
        {
            int currentRefCount;
            int newRefCount;

            // Use atomic compare-and-swap to safely increment the reference count,
            // ensuring consistency even with concurrent modifications by other threads.
            do
            {
                currentRefCount = _refs;

                // Prevent adding a reference to an invalid or already disposed state,
                // protecting against undefined behavior and ensuring references are only added to valid, active objects.
                if (currentRefCount < 0)
                    ThrowInvalidPagerState();

                newRefCount = currentRefCount + 1;
            } 
            while (Interlocked.CompareExchange(ref _refs, newRefCount, currentRefCount) != currentRefCount);

#if DEBUG_PAGER_STATE
            // Tracks the call stack for debugging reference additions,
            // helping developers diagnose where and how references are being manipulated, aiding in debugging reference management issues.

            AddedRefs.Enqueue(Environment.StackTrace);
            while (AddedRefs.Count > 500)
            {
                String trace;
                AddedRefs.TryDequeue(out trace);
            }
#endif
        }

        [DoesNotReturn]
        internal void ThrowInvalidPagerState()
        {
            throw new ObjectDisposedException("Cannot add reference to a disposed pager state for " + _pager.FileName);
        }

        [Conditional("VALIDATE")]
        public void DebugVerify(long size)
        {
            if (AllocationInfos == null)
                return;

            foreach (var allocationInfo in AllocationInfos)
            {
                _pager.UnprotectPageRange(allocationInfo.BaseAddress, (ulong)allocationInfo.Size);

                for (long i = 0; i < allocationInfo.Size; i++)
                {
                    var b = *(allocationInfo.BaseAddress + i);
                    *(allocationInfo.BaseAddress + i) = b;
                }

                _pager.ProtectPageRange(allocationInfo.BaseAddress, (ulong)allocationInfo.Size);
            }

            _pager.UnprotectPageRange(MapBase, (ulong)size);

            for (long i = 0; i < size; i++)
            {
                var b = *(MapBase + i);
                *(MapBase + i) = b;
            }

            _pager.ProtectPageRange(MapBase, (ulong)size);
        }

        public AbstractPager CurrentPager => _pager;


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetSegmentState(long segment)
        {
            if (segment < 0 || segment > _prefetchTable.Length)
                return AlreadyPrefetch;

            byte value = _prefetchTable[segment / 2];
            if (segment % 2 == 0)
            {
                // The actual value is in the high byte.
                value = (byte)(value >> EvenPrefetchMaskShift);
            }
            else
            {
                value = (byte)(value & OddPrefetchCountMask);
            }

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetSegmentState(long segment, int state)
        {
            byte value = this._prefetchTable[segment / 2];
            if (segment % 2 == 0)
            {
                // The actual value is in the high byte.
                value = (byte)((value & OddPrefetchCountMask) | (state << EvenPrefetchMaskShift));
            }
            else
            {
                value = (byte)((value & EvenPrefetchCountMask) | state);
            }

            this._prefetchTable[segment / 2] = value;
        }

        public bool ShouldPrefetchSegment(long pageNumber, out void* virtualAddress, out long sizeInBytes)
        {
            long segmentNumber = (pageNumber * Constants.Storage.PageSize) >> this._segmentShift;

            int segmentState = GetSegmentState(segmentNumber);
            if (segmentState < AlreadyPrefetch)
            {
                // We update the current segment counter
                segmentState++;

                // if the previous or next segments were loaded, eagerly
                // load this one, probably a sequential scan of one type or
                // another
                int previousSegmentState = GetSegmentState(segmentNumber - 1);
                if (previousSegmentState == AlreadyPrefetch)
                {
                    segmentState = AlreadyPrefetch;
                }
                else
                {
                    int nextSegmentState = GetSegmentState(segmentNumber + 1);
                    if (nextSegmentState == AlreadyPrefetch)
                    {
                        segmentState = AlreadyPrefetch;
                    }
                }

                SetSegmentState(segmentNumber, segmentState);

                if (segmentState == AlreadyPrefetch)
                {
                    _refreshCounter++;

                    // Prepare the segment information. 
                    sizeInBytes = _prefetchSegmentSize;
                    virtualAddress = this.MapBase + segmentNumber * _prefetchSegmentSize;
                    return true;
                }
            }

            sizeInBytes = 0;
            virtualAddress = null;
            return false;
        }

        public void CheckResetPrefetchTable()
        {
            if (_refreshCounter > this._prefetchResetThreshold)
            {
                this._refreshCounter = 0;

                // We will zero out the whole table to reset the prefetching behavior. 
                Array.Clear(this._prefetchTable, 0, this._prefetchTable.Length);
            }
        }

        public void CopyPrefetchState(PagerState olderInstance)
        {
            // this is called from AllocateMorePages and is used to copy the current state of the
            // prefetch state of the file. Our own size will be larger than the previous one. 
            Array.Copy(olderInstance._prefetchTable, this._prefetchTable, olderInstance._prefetchTable.Length);
        }

        public void DiscardDataOnDisk()
        {
            if (_pager.Options.DiscardVirtualMemory == false)
                return;

            if (AllocationInfos != null)
                Pal.rvn_discard_virtual_memory(AllocationInfos[0].BaseAddress, AllocationInfos[0].Size, out _);
        }
    }
}
