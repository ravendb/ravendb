#nullable enable

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Server.Meters;
using Sparrow.Server.Platform;
using Voron.Exceptions;
using Voron.Global;

namespace Voron.Impl.Paging;



public unsafe partial class Pager2 : IDisposable
{
    public readonly string FileName;
    public uint UniquePhysicalDriveId;
    public readonly StorageEnvironmentOptions Options;
    
    private readonly bool _canPrefetch, _temporaryOrDeleteOnClose, _usePageProtection;

    private readonly StateFor32Bits? _32BitsState;

    private class StateFor32Bits
    {
        public readonly ReaderWriterLockSlim AllocationLock = new ReaderWriterLockSlim();
        public readonly ConcurrentDictionary<long, ConcurrentSet<MappedAddresses>> MemoryMapping = new();
    }

    private readonly Functions _functions;
    private readonly ConcurrentSet<WeakReference<State>> _states = [];
    private readonly EncryptionBuffersPool _encryptionBuffersPool;
    private readonly byte[] _masterKey;
    private readonly PrefetchTableHint _prefetchState;
    private readonly Logger _logger;
    private DateTime _lastIncrease;
    private long _increaseSize;
    
    public const int MinIncreaseSize = 16 * Constants.Size.Kilobyte;
    public const int MaxIncreaseSize = Constants.Size.Gigabyte;

    public struct OpenFileOptions
    {
        public string File;
        public long? InitializeFileSize;
        public bool Temporary;
        public bool DeleteOnClose;
        public bool ReadOnly;
        public bool SequentialScan;
        public bool UsePageProtection;
    }

    public static (Pager2 Pager, State State) Create(StorageEnvironmentOptions options, string file)
    {
        return Create(options, new OpenFileOptions
        {
            File =  file,
            Temporary = false,
            DeleteOnClose = false,
            SequentialScan = true,
            ReadOnly = false,
        });
    }
    
    public static (Pager2 Pager, State State) Create(StorageEnvironmentOptions options, OpenFileOptions openFileOptions)
    {
        var funcs = options.RunningOn32Bits switch
        {
            false when PlatformDetails.RunningOnWindows => Win64.CreateFunctions(),
            true when PlatformDetails.RunningOnWindows => Win32.CreateFunctions(),
            _ => throw new NotSupportedException("Running " + RuntimeInformation.OSDescription)
        };

        if (options.Encryption.IsEnabled)
        {
            funcs.AcquirePagePointer = &Crypto.AcquirePagePointer;
            funcs.AcquirePagePointerForNewPage = &Crypto.AcquirePagePointerForNewPage;
        }
        
        var pager = new Pager2(options, openFileOptions, funcs, canPrefetchAhead: true, usePageProtection: openFileOptions.UsePageProtection,
            out State state);

        return (pager, state);
    }

    private Pager2(StorageEnvironmentOptions options, 
        OpenFileOptions openFileOptions,
        Functions functions, 
        bool canPrefetchAhead,
        bool usePageProtection,
        out State state)
    {
        Options = options;
        FileName = openFileOptions.File;
        
        _logger = LoggingSource.Instance.GetLogger<StorageEnvironment>($"Pager-{openFileOptions}");
        _canPrefetch = PlatformDetails.CanPrefetch && canPrefetchAhead && options.EnablePrefetching;
        _temporaryOrDeleteOnClose = openFileOptions.Temporary || openFileOptions.DeleteOnClose; 
        _usePageProtection = usePageProtection;
        _encryptionBuffersPool = options.Encryption.EncryptionBuffersPool;
        _masterKey = options.Encryption.MasterKey;
        _functions = functions;
        _increaseSize = MinIncreaseSize;
        state = functions.Init(this, openFileOptions);
        _prefetchState = new PrefetchTableHint(options.PrefetchSegmentSize, options.PrefetchResetThreshold, state.TotalAllocatedSize);
        if (options.RunningOn32Bits)
        {
            _32BitsState = new StateFor32Bits();
        }
    }
    
    public void DirectWrite(ref State state, ref PagerTransactionState txState,long posBy4Kbs, int numberOf4Kbs, byte* source)
    {
        const int pageSizeTo4KbRatio = (Constants.Storage.PageSize / (4 * Constants.Size.Kilobyte));
        var pageNumber = posBy4Kbs / pageSizeTo4KbRatio;
        var offsetBy4Kb = posBy4Kbs % pageSizeTo4KbRatio;
        var numberOfPages = numberOf4Kbs / pageSizeTo4KbRatio;
        if (numberOf4Kbs % pageSizeTo4KbRatio != 0)
            numberOfPages++;

        EnsureContinuous(ref state, pageNumber, numberOfPages);

        var toWrite = numberOf4Kbs * 4 * Constants.Size.Kilobyte;
        EnsureMapped(state, ref txState, pageNumber, numberOfPages);
        byte* destination = AcquireRawPagePointer(state, ref txState, pageNumber)
                            + (offsetBy4Kb * 4 * Constants.Size.Kilobyte);

        UnprotectPageRange(destination, (ulong)toWrite);

        Memory.Copy(destination, source, toWrite);

        ProtectPageRange(destination, (ulong)toWrite);
    }
    
    public void DiscardWholeFile(State state)
    {
        DiscardPages(state, 0, state.NumberOfAllocatedPages);
    }
    public void DiscardPages(State state, long pageNumber, long numberOfPages)
    {
        if (Options.DiscardVirtualMemory == false)
            return;
            
        byte* baseAddress = state.BaseAddress;
        long offset = pageNumber * Constants.Storage.PageSize;

        _ = Pal.rvn_discard_virtual_memory(baseAddress + offset, numberOfPages * Constants.Storage.PageSize, out _);
    }

    public void TryPrefetchingWholeFile(State state)
    {
        MaybePrefetchMemory(state, 0, state.NumberOfAllocatedPages);
    }

    public bool EnsureMapped(State state, ref PagerTransactionState txState, long page, int numberOfPages)
    {
        return _functions.EnsureMapped(this, state, ref txState, page, numberOfPages);
    }

    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MaybePrefetchMemory(State state, long pageNumber, long pagesToPrefetch)
    {
        MaybePrefetchMemory(state, new AbstractPager.PageIterator(pageNumber, pagesToPrefetch));
    }
    
    public void MaybePrefetchMemory<T>(State state, T pagesToPrefetch) where T : struct, IEnumerator<long>
    {
        if (!_canPrefetch)
            return;

        if (pagesToPrefetch.MoveNext() == false)
            return;

        var prefetcher = GlobalPrefetchingBehavior.GlobalPrefetcher.Value;

        do
        {
            long pageNumber = pagesToPrefetch.Current;
            if (!_prefetchState.ShouldPrefetchSegment(pageNumber, out var offset, out long bytes)) 
                continue;
            
            prefetcher.CommandQueue.TryAdd(new PalDefinitions.PrefetchRanges
            {
                VirtualAddress = state.BaseAddress + offset,
                NumberOfBytes = (nint)bytes
            }, 0);
        }
        while (pagesToPrefetch.MoveNext());

        _prefetchState.CheckResetPrefetchTable();
    }


    public void Sync(State state, long totalUnsynced)
    {
        if (state.Disposed || _temporaryOrDeleteOnClose)
            return; // nothing to do here
        
        using var metric = Options.IoMetrics.MeterIoRate(FileName, IoMetrics.MeterType.DataSync, 0);
        metric.IncrementFileSize(state.TotalAllocatedSize);
        _functions.Sync(this, state);
        metric.IncrementSize(totalUnsynced);
    }
    
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte* AcquirePagePointerWithOverflowHandling(State state, ref PagerTransactionState txState, long pageNumber)
    {
        // Case 1: Page is not overflow ==> no problem, returning a pointer to existing mapping
        var pageHeader = (PageHeader*)AcquirePagePointer(state, ref txState, pageNumber);
        if ((pageHeader->Flags & PageFlags.Overflow) != PageFlags.Overflow)
            return (byte*)pageHeader;

        // Case 2: Page is overflow and already mapped large enough ==> no problem, returning a pointer to existing mapping
        if (EnsureMapped(state, ref txState, pageNumber, VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize)) == false)
            return (byte*)pageHeader;

        // Case 3: Page is overflow and was ensuredMapped above, view was re-mapped so we need to acquire a pointer to the new mapping.
        return AcquirePagePointer(state, ref txState, pageNumber);
    }
    
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private byte* AcquireRawPagePointerWithOverflowHandling(State state, ref PagerTransactionState txState, long pageNumber)
    {
        // Case 1: Page is not overflow ==> no problem, returning a pointer to existing mapping
        var pageHeader = (PageHeader*)AcquireRawPagePointer(state, ref txState, pageNumber);
        if ((pageHeader->Flags & PageFlags.Overflow) != PageFlags.Overflow)
            return (byte*)pageHeader;

        // Case 2: Page is overflow and already mapped large enough ==> no problem, returning a pointer to existing mapping
        if (EnsureMapped(state, ref txState, pageNumber, VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize)) == false)
            return (byte*)pageHeader;

        // Case 3: Page is overflow and was ensuredMapped above, view was re-mapped so we need to acquire a pointer to the new mapping.
        return AcquireRawPagePointer(state, ref txState, pageNumber);
    }
    
    public byte* AcquirePagePointer(State state, ref PagerTransactionState txState, long pageNumber)
    {
        if (pageNumber <= state.NumberOfAllocatedPages && pageNumber >= 0)
            return _functions.AcquirePagePointer(this, state, ref txState, pageNumber);
        
        VoronUnrecoverableErrorException.Raise(Options,
            "The page " + pageNumber + " was not allocated, allocated pages: " + state.NumberOfAllocatedPages + " in " + FileName);
        return null;// never hit
    }

    private byte* AcquireRawPagePointer(State state, ref PagerTransactionState txState, long pageNumber)
    {
        if (pageNumber <= state.NumberOfAllocatedPages && pageNumber >= 0)
            return _functions.AcquireRawPagePointer(this, state, ref txState, pageNumber);
        
        VoronUnrecoverableErrorException.Raise(Options,
            "The page " + pageNumber + " was not allocated, allocated pages: " + state.NumberOfAllocatedPages + " in " + FileName);
        return null;// never hit
    }

    public byte* AcquirePagePointerForNewPage(State state, ref PagerTransactionState txState, long pageNumber, int numberOfPages)
    {
        return _functions.AcquirePagePointer(this, state, ref txState, pageNumber);
    }

    public void ProtectPageRange(byte* start, ulong size)
    {
        if (_usePageProtection == false || size == 0)
            return;

        _functions.ProtectPageRange(start, size);
    }
    
    public void UnprotectPageRange(byte* start, ulong size)
    {
        if (_usePageProtection == false || size == 0)
            return;

        _functions.UnprotectPageRange(start, size);
    }
    
    public void EnsureContinuous(ref State state, long requestedPageNumber, int numberOfPages)
    {
        if (state.Disposed)
            throw new ObjectDisposedException("PagerState was already disposed");
        
        if (requestedPageNumber + numberOfPages <= state.NumberOfAllocatedPages)
            return;
        
        // this ensures that if we want to get a range that is more than the current expansion
        // we will increase as much as needed in one shot
        var minRequested = (requestedPageNumber + numberOfPages) * Constants.Storage.PageSize;
        var allocationSize = Math.Max(state.NumberOfAllocatedPages * Constants.Storage.PageSize, Constants.Storage.PageSize);
        while (minRequested > allocationSize)
        {
            allocationSize = GetNewLength(allocationSize, minRequested);
        }

        if (Options.CopyOnWriteMode && state.Pager.FileName.EndsWith(Constants.DatabaseFilename))
            throw new IncreasingDataFileInCopyOnWriteModeException(state.Pager.FileName, allocationSize);

        _functions.AllocateMorePages(this, allocationSize, ref state);
    }


    private long GetNewLength(long current, long minRequested)
    {
        DateTime now = DateTime.UtcNow;
        if (_lastIncrease == DateTime.MinValue)
        {
            _lastIncrease = now;
            return MinIncreaseSize;
        }

        if (LowMemoryNotification.Instance.InLowMemory)
        {
            _lastIncrease = now;
            // cannot return less than the minRequested
            return GetNearestFileSize(minRequested);
        }

        TimeSpan timeSinceLastIncrease = (now - _lastIncrease);
        _lastIncrease = now;
        if (timeSinceLastIncrease.TotalMinutes < 3)
        {
            _increaseSize = Math.Min(_increaseSize * 2, MaxIncreaseSize);
        }
        else if (timeSinceLastIncrease.TotalMinutes > 15)
        {
            _increaseSize = Math.Max(MinIncreaseSize, _increaseSize / 2);
        }

        // At any rate, we won't do an increase by over 50% of current size, to prevent huge empty spaces
        // 
        // The reasoning behind this is that we want to make sure that we increase in size very slowly at first
        // because users tend to be sensitive to a lot of "wasted" space. 
        // We also consider the fact that small increases in small files would probably result in cheaper costs, and as
        // the file size increases, we will reserve more & more from the OS.
        // This also avoids "I added 300 records and the file size is 64MB" problems that occur when we are too
        // eager to reserve space
        var actualIncrease = Math.Min(_increaseSize, current / 2);

        // we then want to get the next power of two number, to get pretty file size
        var totalSize = current + actualIncrease;
        return GetNearestFileSize(totalSize);
    }

    private static readonly long IncreaseByPowerOf2Threshold = new Size(512, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes);

    private static long GetNearestFileSize(long neededSize)
    {
        if (neededSize < IncreaseByPowerOf2Threshold)
            return Bits.PowerOf2(neededSize);

        // if it is over 0.5 GB, then we grow at 1 GB intervals
        var remainder = neededSize % Constants.Size.Gigabyte;
        if (remainder == 0)
            return neededSize;

        // above 0.5GB we need to round to the next GB number
        return neededSize + Constants.Size.Gigabyte - remainder;
    }

    private void InstallState(State state)
    {
        _states.Add(state.WeakSelf);
    }

    private class PrefetchTableHint
    {
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

        public PrefetchTableHint(long prefetchSegmentSize, long prefetchResetThreshold, long initialFileSize)
        {
            _segmentShift = Bits.MostSignificantBit(prefetchSegmentSize);

            _prefetchSegmentSize = 1 << _segmentShift;
            _prefetchResetThreshold = (int)((float)prefetchResetThreshold / _prefetchSegmentSize);

            Debug.Assert((_prefetchSegmentSize - 1) >> _segmentShift == 0);
            Debug.Assert(_prefetchSegmentSize >> _segmentShift == 1);

            long numberOfAllocatedSegments = (initialFileSize / _prefetchSegmentSize) + 1;
            _prefetchTable = new byte[(numberOfAllocatedSegments / 2) + 1];
        }
        
        
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

        public bool ShouldPrefetchSegment(long pageNumber, out long offsetFromFileBase, out long sizeInBytes)
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
                    offsetFromFileBase =  segmentNumber * _prefetchSegmentSize;
                    return true;
                }
            }

            sizeInBytes = 0;
            offsetFromFileBase = 0;
            return false;
        }
        
        public void CheckResetPrefetchTable()
        {
            if (_refreshCounter > _prefetchResetThreshold)
            {
                _refreshCounter = 0;

                // We will zero out the whole table to reset the prefetching behavior. 
                Array.Clear(_prefetchTable, 0, this._prefetchTable.Length);
            }
        }
    }

    public void Dispose()
    {
        foreach (WeakReference<State> state in _states)
        {
            if (state.TryGetTarget(out var v))
            {
                v.Dispose();
            }
        }
    }
}
