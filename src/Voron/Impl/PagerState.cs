using System;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Binary;
using Voron.Global;
using Voron.Impl.Paging;
using Voron.Platform;


namespace Voron.Impl
{
    public sealed unsafe class PagerState
    {
        private readonly AbstractPager _pager;

        public bool DisposeFileOnDispose = true;

        public class AllocationInfo
        {
            public MemoryMappedFile MappedFile;
            public byte* BaseAddress;
            public long Size;

            public override string ToString()
            {
                return $"{nameof(BaseAddress)}: {new IntPtr(BaseAddress).ToString("x")} - {new IntPtr(BaseAddress + Size).ToString("x")} {nameof(Size)}: {Size}";
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

        private bool _released;

        public void Release()
        {
            if (Interlocked.Decrement(ref _refs) != 0)
                return;


#if DEBUG_PAGER_STATE
            String value;
            Instances.TryRemove(this, out value);
#endif

            ReleaseInternal();
        }

        private void ReleaseInternal()
        {
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

            _released = true;
        }

#if DEBUG_PAGER_STATE
        public ConcurrentQueue<string> AddedRefs = new ConcurrentQueue<string>();
#endif
        public void AddRef()
        {
            if (_released)
                ThrowInvalidPagerState();

            Interlocked.Increment(ref _refs);
#if DEBUG_PAGER_STATE
            AddedRefs.Enqueue(Environment.StackTrace);
            while (AddedRefs.Count > 500)
            {
                String trace;
                AddedRefs.TryDequeue(out trace);
            }
#endif
        }

        private void ThrowInvalidPagerState()
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
            if (AllocationInfos != null)
                Pal.rvn_discard_virtual_memory(AllocationInfos[0].BaseAddress, AllocationInfos[0].Size, out _);
        }
    }
}
