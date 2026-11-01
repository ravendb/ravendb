#nullable enable

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Server.Exceptions;
using Sparrow.Server.Meters;
using Sparrow.Server.Platform;
using Sparrow.Server.Platform.Posix;
using Sparrow.Server.Utils;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl.Journal;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Voron.Impl.Paging;

public unsafe partial class Pager : IDisposable
{
    public readonly string FileName;
    public readonly StorageEnvironmentOptions Options;
    private readonly Pal.OpenFileFlags _flags;
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
    private SegmentPrefetchTable _prefetchTable;
    internal DeviceWriteBudget? DeviceBudget { get; private set; }
    private long _prefetchRequestSize;
    private PrefetchRing? _prefetchRing;
    private long _prefetchTicket;
    private DateTime _lastIncrease;
    private long _increaseSize;

    private const int MinIncreaseSize = 16 * Constants.Size.Kilobyte;
    private const int MaxIncreaseSize = Constants.Size.Gigabyte;

    public static (Pager Pager, State State) Create(StorageEnvironmentOptions options, string filename, long initialFileSize, Pal.OpenFileFlags flags, int? pageSize = null)
    {
        if (Pal.PalVoronPageSize != Constants.Storage.PageSize)// JIT should eliminate this
            throw new InvalidOperationException($"Expected the PAL to have page size matching Voron but was: {Pal.PalVoronPageSize}");
        
        var pager = new Pager(options, filename, flags, GetFunctions(options, flags));
        var ring = flags.HasFlag(Pal.OpenFileFlags.Prefetch) ? options.PrefetchRing ?? PrefetchRing.Default : null;
        var result = Pal.rvn_init_pager(filename, initialFileSize, flags, ring != null ? ring.Handle : null,
            out var handle, out var readOnlyMemory, out var writeMemory, out var memorySize, out var error);
        if (result == PalFlags.FailCodes.FailPrefetchFilesExhausted)
            throw new InvalidOperationException(
                $"Cannot open '{filename}': {ring!.MaxFiles} data files are already open with the prefetch I/O ring, which is its limit. " +
                "Raise the 'Storage.PrefetchRingMaxFiles' configuration option (a server restart is needed) to open more databases and indexes.");
        if (result != PalFlags.FailCodes.Success)
            RaiseError(filename, error, result, initialFileSize);

        pager.Write = Pal.rvn_get_writer(handle);
        var state = new State(pager, readOnlyMemory, writeMemory, memorySize, handle, pageSize ?? options.PageSize);
        (state.TotalAllocatedSize, state.TotalPhysicalSpace) = pager.GetFileSize(state);
        pager.InstallState(state);
        if (Pal.rvn_pager_get_device_id(handle, out var deviceId, out _) == PalFlags.FailCodes.Success)
            pager.DeviceBudget = DeviceWriteBudget.GetForDevice(deviceId, filename, options.SyncWritebackBarrierCostThresholdTicks,
                options.SyncWritebackDrainQueueDepthThreshold, options.PipelineJournalWritesAboveLatencyInTicks);
        pager.Initialize(memorySize);
        pager._prefetchRing = ring is { IsAvailable: true } ? ring : null;
        Pal.rvn_pager_get_prefetch_info(handle, out var prefetchInfo);
        pager._prefetchRequestSize = prefetchInfo.RequestSize;
        pager._prefetchTicket = prefetchInfo.Ticket;
        return (pager, state);
    }

    public Pal.WriterFunc Write = WriterNotSetupYet;

    private static PalFlags.FailCodes WriterNotSetupYet(void* handle, Pal.page_to_write* buffers, int count, out int errorCode)
    {
        throw new InvalidOperationException("The Writing function is setup at this point in time, this should never happen");
    }

    private static Functions GetFunctions(StorageEnvironmentOptions options, Pal.OpenFileFlags flags)
    {
        var funcs = options.RunningOn32Bits ? Bits32.CreateFunctions() : Bits64.CreateFunctions();
        if (flags.HasFlag(Pal.OpenFileFlags.Encrypted))
        {
            funcs.AcquirePagePointer = &Crypto.AcquirePagePointer;
            funcs.AcquirePagePointerForNewPage = &Crypto.AcquirePagePointerForNewPage;
            funcs.ConvertToWritePointer = &Crypto.ConvertToWritePointer;
        }
        return funcs;
    }

    public static void RaiseError(string filename, int errorCode, PalFlags.FailCodes rc, long initialFileSize, [CallerMemberName] string? caller = null)
    {
        if (rc == PalFlags.FailCodes.FailCreateIoRing)
        {
            if (Pal.rvn_get_error_meaning(errorCode) is PalFlags.ErrnoSpecialCodes.NoMem)
            {
                string msg = $"Failed to create IoRing because out of of memory error. This usually indicate that the kernel was unable to lock sufficient memory to create the ring.{Environment.NewLine}";
                if (PlatformDetails.RunningOnLinux)
                {
                    msg +=
                        $"Check 'ulimit -l' for the actual value you have configured. You can raise the amount of memory you can lock using:{Environment.NewLine}" +
                        $"sudo prlimit --memlock=unlimited:unlimited --pid $${Environment.NewLine}";
                }
                throw new InsufficientMemoryException(msg);
            }
        }
        
        if (rc == PalFlags.FailCodes.FailLockMemory)
            throw new InsufficientMemoryException(
                $"Failed to increase the min working set size so we can lock memory for {filename}. With encrypted " +
                "databases we lock some memory in order to avoid leaking secrets to disk. Treating this as a catastrophic error " +
                "and aborting the current operation.");

        try
        {
            PalHelper.ThrowLastError(rc, errorCode, $"{caller} failed on {rc} for '{filename}'");
        }
        catch (DiskFullException dfEx)
        {
            var diskSpaceResult = DiskUtils.GetDiskSpaceInfoForFile(filename);
            throw new DiskFullException(filename, initialFileSize, diskSpaceResult?.TotalFreeSpace.GetValue(SizeUnit.Bytes), dfEx.Message);
        }
    }

    private Pager(StorageEnvironmentOptions options,
        string filename,
        Pal.OpenFileFlags flags,
        Functions functions)
    {
        Options = options;
        _flags = flags;
        FileName = filename;
        _canPrefetch = PlatformDetails.CanPrefetch && options.EnablePrefetching;
        _encryptionBuffersPool = options.Encryption.EncryptionBuffersPool;
        _masterKey = options.Encryption.MasterKey;
        _functions = functions;
        _increaseSize = MinIncreaseSize;
        if (options.RunningOn32Bits)
        {
            _32BitsState = new StateFor32Bits();
        }
    }

    private void Initialize(long initialSize)
    {
        _prefetchTable = new SegmentPrefetchTable(PrefetchPolicy.Global, DeviceBudget, Options.PrefetchSegmentSize, initialSize);
    }

    public void DiscardWholeFile(State state)
    {
        DiscardPages(state, 0, state.NumberOfAllocatedPages);
    }

    public void DiscardPages(State state, long pageNumber, long numberOfPages)
    {
        if (Options.DiscardVirtualMemory == false)
            return;

        byte* baseAddress = state.ReadAddress;
        long offset = pageNumber * Constants.Storage.PageSize;

        _ = Pal.rvn_discard_virtual_memory(baseAddress + offset, numberOfPages * Constants.Storage.PageSize, out _);
    }

    public void TrySetSequentialReadAheadHint(State state)
    {
        // posix_fadvise(POSIX_FADV_SEQUENTIAL) is Linux-only (absent on macOS). On Windows the read-ahead is
        // handled at file-open time, so the hint is a no-op there - the caller gates this on RunningOnLinux too.
        if (PlatformDetails.RunningOnLinux == false)
            return;

        // PAL hands back a dup of the pager's fd, so disposing the handle below does not close the pager's file.
        if (Pal.rvn_pager_get_file_handle(state.Handle, out var fileHandle, out _) != PalFlags.FailCodes.Success)
            return;

        using (fileHandle)
        {
            var fd = fileHandle.DangerousGetHandle().ToInt32();
            if (fd < 0)
                return;

            // best-effort kernel read-ahead hint for the upcoming sequential scan; failures are ignored
            Syscall.posix_fadvise(fd, 0, 0, Syscall.POSIX_FADV_SEQUENTIAL);
        }
    }

    public bool EnsureMapped(State state, ref PagerTransactionState txState, long page, int numberOfPages)
    {
        if (_functions.EnsureMapped == null)
            return false;
        
        return _functions.EnsureMapped(this, state, ref txState, page, numberOfPages);
    }

    public void MaybePrefetchMemory<T>(State state, T pagesToPrefetch) where T : struct, IEnumerator<long>
    {
        if (!_canPrefetch)
            return;

        if (pagesToPrefetch.MoveNext() == false)
            return;

        do
        {
            long pageNumber = pagesToPrefetch.Current;
            if (_prefetchTable.ShouldPrefetchSegment(pageNumber, out var offset, out long bytes))
                PrefetchSegment(state, offset, bytes);
        } while (pagesToPrefetch.MoveNext());
    }

    private void PrefetchSegment(State state, long offset, long bytes)
    {
        if (PlatformDetails.RunningOnLinux)
            Pal.rvn_pager_prefetch(state.Handle, offset, bytes);
        else
            GlobalPrefetchingBehavior.GlobalPrefetcher.Value.CommandQueue.TryAdd(new PalDefinitions.PrefetchRanges(state.ReadAddress + offset, bytes, _prefetchRequestSize), 0);
    }


    public void Sync(State state, long totalUnsynced)
    {
        if (state.Disposed || _flags.HasFlag(Pal.OpenFileFlags.Temporary))
            return; // nothing to do here

        using var metric = Options.IoMetrics.MeterIoRate(FileName, IoMetrics.MeterType.DataSync, 0);
        metric.IncrementFileSize(state.TotalAllocatedSize);
        var rc = Pal.rvn_sync_pager(state.Handle, out var errorCode);
        if (rc != PalFlags.FailCodes.Success)
        {
            PalHelper.ThrowLastError(rc, errorCode, $"Failed to sync file: {FileName}");
        }
        metric.IncrementSize(totalUnsynced);
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte* AcquirePagePointerWithOverflowHandling(State state, ref PagerTransactionState txState, long pageNumber)
    {
        var pageHeader = (PageHeader*)AcquirePagePointer(state, ref txState, pageNumber);
        ThrowIfOverflowExtentExceedsAllocatedPages(state, pageNumber, pageHeader);

        if (_functions.EnsureMapped == null) 
            return (byte*)pageHeader;

        return EnsureOverflowPageIsLoaded(state, ref txState, pageNumber, pageHeader);
    }

    private byte* EnsureOverflowPageIsLoaded(State state, ref PagerTransactionState txState, long pageNumber, PageHeader* pageHeader)
    {
        // Case 1: Page is not overflow ==> no problem, returning a pointer to existing mapping
        if ((pageHeader->Flags & PageFlags.Overflow) != PageFlags.Overflow)
            return (byte*)pageHeader;

        // Case 2: Page is overflow and already mapped large enough ==> no problem, returning a pointer to existing mapping
        if (EnsureMapped(state, ref txState, pageNumber, Paging.GetNumberOfOverflowPages(pageHeader->OverflowSize)) == false)
            return (byte*)pageHeader;

        // Case 3: Page is overflow and was ensuredMapped above, view was re-mapped so we need to acquire a pointer to the new mapping.
        return _functions.AcquirePagePointer(this, state, ref txState, pageNumber);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte* AcquireRawPagePointerWithOverflowHandling(State state, ref PagerTransactionState txState, long pageNumber)
    {
        var pageHeader = (PageHeader*)AcquireRawPagePointer(state, ref txState, pageNumber);
        ThrowIfOverflowExtentExceedsAllocatedPages(state, pageNumber, pageHeader);

        if (_functions.EnsureMapped == null)
            return (byte*)pageHeader;

        return EnsureOverflowRawPageIsLoaded(state, ref txState, pageNumber, pageHeader);
    }

    private byte* EnsureOverflowRawPageIsLoaded(State state, ref PagerTransactionState txState, long pageNumber, PageHeader* pageHeader)
    {
        // Case 1: Page is not overflow ==> no problem, returning a pointer to existing mapping
        if ((pageHeader->Flags & PageFlags.Overflow) != PageFlags.Overflow)
            return (byte*)pageHeader;
        // Case 2: Page is overflow and already mapped large enough ==> no problem, returning a pointer to existing mapping
        if (EnsureMapped(state, ref txState, pageNumber, Paging.GetNumberOfOverflowPages(pageHeader->OverflowSize)) == false)
            return (byte*)pageHeader;

        // Case 3: Page is overflow and was ensuredMapped above, view was re-mapped so we need to acquire a pointer to the new mapping.
        return _functions.AcquireRawPagePointer(this, state, ref txState, pageNumber);
    }

    public byte* AcquirePagePointer(State state, ref PagerTransactionState txState, long pageNumber)
    {
        if (pageNumber <= state.NumberOfAllocatedPages && pageNumber >= 0)
            return _functions.AcquirePagePointer(this, state, ref txState, pageNumber);
        return ThrowInvalidPage(state, pageNumber);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private byte* ThrowInvalidPage(State state, long pageNumber)
    {
        VoronUnrecoverableErrorException.Raise(Options,
            "The page " + pageNumber + " was not allocated, allocated pages: " + state.NumberOfAllocatedPages + " in " + FileName);
        return null; // never hit
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void ThrowIfOverflowExtentExceedsAllocatedPages(State state, long pageNumber, PageHeader* pageHeader)
    {
        // OverflowSize is only defined on overflow pages, other page types hold their own fields at that offset
        if ((pageHeader->Flags & PageFlags.Overflow) != PageFlags.Overflow)
            return;

        // the persisted size is untrusted, its extent must fit the allocated file
        int overflowSize = pageHeader->OverflowSize;
        if (overflowSize < 0 || pageNumber + Paging.GetNumberOfOverflowPages(overflowSize) > state.NumberOfAllocatedPages)
            ThrowOverflowExtentExceedsAllocatedPages(state, pageNumber, overflowSize);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ThrowOverflowExtentExceedsAllocatedPages(State state, long pageNumber, int overflowSize)
    {
        VoronUnrecoverableErrorException.Raise(Options,
            "The overflow page " + pageNumber + " has an invalid overflow size of " + overflowSize +
            " bytes, spanning " + Paging.GetNumberOfOverflowPages(overflowSize) + " pages, but only " +
            state.NumberOfAllocatedPages + " pages are allocated in " + FileName);
    }

    public byte* AcquireRawPagePointer(State state, ref PagerTransactionState txState, long pageNumber)
    {
        if (pageNumber <= state.NumberOfAllocatedPages && pageNumber >= 0)
            return _functions.AcquireRawPagePointer(this, state, ref txState, pageNumber);

        return ThrowInvalidPage(state, pageNumber);
    }

    public byte* AcquirePagePointerForNewPage(State state, ref PagerTransactionState txState, long pageNumber, int numberOfPages)
    {
        return _functions.AcquirePagePointerForNewPage(this, pageNumber, numberOfPages, state, ref txState);
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
        Debug.Assert(allocationSize > state.TotalAllocatedSize, "allocationSize > state.TotalAllocatedSize");
        
        if (Options.CopyOnWriteMode && state.Pager.FileName.EndsWith(Constants.DatabaseFilename))
            throw new IncreasingDataFileInCopyOnWriteModeException(state.Pager.FileName, allocationSize);

        var rc = Pal.rvn_increase_pager_size(state.Handle, 
            allocationSize, out var handle, out var readAddress, out var writeAddress, 
            out var totalAllocatedSize, out var errorCode);

        if (rc != PalFlags.FailCodes.Success)
        {
            PalHelper.ThrowLastError(rc, errorCode, $"Failed to increase file '{state.Pager.FileName}' to {new Size(allocationSize, SizeUnit.Bytes)}");
        }
        Debug.Assert(totalAllocatedSize >= state.TotalAllocatedSize, "totalAllocatedSize >= state.TotalAllocatedSize");
        state = new State(this, readAddress, writeAddress, totalAllocatedSize, handle, pageSize: Options.PageSize);
        InstallState(state);
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
        var actualIncrease = Math.Min(_increaseSize, current >= IncreaseByPowerOf2Threshold ? current : current / 2);

        // we then want to get the next power of two number, to get pretty file size
        var totalSize = current + actualIncrease;
        return GetNearestFileSize(totalSize);
    }

    private static readonly long IncreaseByPowerOf2Threshold = new Size(512, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes);
    private readonly bool _canPrefetch;

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

    public void Dispose()
    {
        foreach (WeakReference<State> state in _states)
        {
            if (state.TryGetTarget(out var v))
            {
                v.Dispose();
            }
        }

        if (PlatformDetails.RunningOnPosix &&
            Options.OwnsPagers &&
            _flags.HasFlag(Pal.OpenFileFlags.Temporary))
        {
            // Posix doesn't support DeleteOnClose
            try
            {
                File.Delete(FileName);
            }
            catch
            {
                // if we can't delete it, there isn't much that 
                // we can do about it
            }
        }
    }

    public void TryReleasePage(ref PagerTransactionState txState, long pageNumber)
    {
        if (txState.ForCrypto?.TryGetValue(this, out var cyprtoState) is not true)
            return;
        if (cyprtoState.TryGetValue(pageNumber, out var buffer) is not true)
            return;

        if (buffer.Modified)
            return;

        buffer.ReleaseRef();

        if (!buffer.CanRelease) 
            return;
        
        cyprtoState.RemoveBuffer(this,pageNumber);
    }

    public byte* MakeWritable(State state, byte* ptr)
    {
        return _functions.ConvertToWritePointer(this, state, ptr);
    }

    public List<long> GetSparsePages(State state)
    {
        List<long> results = [];
        long offset = 0;
        while (true)
        {
            var rc = Pal.rvn_pager_get_next_sparse_region(state.Handle, offset, out var start, out var size, out var errorCode);
            if (rc is not PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(rc, errorCode, "Failed to get sparse regions for " + state.Pager.FileName);

            if (size == -1 && start == -1)
                break;

            for (long i = 0; i < size; i += Constants.Storage.PageSize)
            {
                results.Add((start + i) / Constants.Storage.PageSize);
            }
            var next = start + size;
            if (next <= offset)
                break;// should never happen
            offset = next;

        }
        return results;
    }

    public void SetSparseRange(State state, long offset, long size)
    {
        var rc = Pal.rvn_pager_set_sparse_region(state.Handle, offset, size, out int errorCode);

        if (rc is PalFlags.FailCodes.Success or
            PalFlags.FailCodes.FailSparseNotSupported)  // explicitly ignoring this
            return;
        
        PalHelper.ThrowLastError(rc, errorCode, "Failed to set sparse range on " + state.Pager.FileName);
    }

    public (long AllocatedSize, long PhysicalSize) GetFileSize(State state)
    {
        var rc = Pal.rvn_pager_get_file_size(state.Handle, out var totalSize, out var physicalSize, out var errorCode);
        if (rc is not PalFlags.FailCodes.Success)
            PalHelper.ThrowLastError(rc, errorCode, "Failed to get file size for " + state.Pager.FileName);

        return (totalSize, physicalSize);
    }

    public override string ToString()
    {
        return FileName;
    }
}
