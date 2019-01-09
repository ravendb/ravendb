using System;
using System.Diagnostics;
using System.IO;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron.Exceptions;
using Voron.Global;
using Voron.Platform;
using Voron.Platform.Win32;
using Voron.Util.Settings;
using static Voron.Platform.Pal;
using static Voron.Platform.PalFlags;

namespace Voron.Impl.Paging
{
    public sealed unsafe class RvnMemoryMapPager : AbstractPager
    {
        public override long TotalAllocationSize => _totalAllocationSize;
        public override int CopyPage(I4KbBatchWrites destwI4KbBatchWrites, long p, PagerState pagerState) => CopyPageImpl(destwI4KbBatchWrites, p, pagerState);
        public override string ToString() => FileName.FullPath;
        protected override string GetSourceName() => $"mmf64: {FileName?.FullPath}";
        private long _totalAllocationSize;
        private readonly bool _copyOnWriteMode;
        private readonly Logger _logger;
        private readonly void* _handle;

        public RvnMemoryMapPager(StorageEnvironmentOptions options, VoronPathSetting file, long? initialFileSize = null, bool canPrefetchAhead = true, bool usePageProtection = false)
            : base(options, canPrefetchAhead, usePageProtection)
        {
            FileName = file;
            _copyOnWriteMode = options.CopyOnWriteMode && FileName.FullPath.EndsWith(Constants.DatabaseFilename);
            _logger = LoggingSource.Instance.GetLogger<StorageEnvironment>($"Pager-{file}");

            if (initialFileSize.HasValue == false || initialFileSize.Value == 0) 
                initialFileSize = Math.Max(SysInfo.PageSize * 16, 64 * 1024);

            initialFileSize += SysInfo.PageSize - initialFileSize % SysInfo.PageSize;

            Debug.Assert(file != null);

            var rc = rvn_create_and_mmap64_file(
                file.FullPath,
                initialFileSize.Value,
                _copyOnWriteMode ? MmapOptions.CopyOnWrite : MmapOptions.None,
                out _handle,
                out var baseAddress,
                out _totalAllocationSize,
                out var errorCode);

            if (rc != 0)
            {
                try
                {
                    PalHelper.ThrowLastError(errorCode, $"rvn_create_and_mmap64_file failed on {(FailCodes)rc}");
                }
                catch (DiskFullException dfEx)
                {
                    var diskSpaceResult = DiskSpaceChecker.GetDiskSpaceInfo(file.FullPath);
                    throw new DiskFullException(file.FullPath, initialFileSize.Value, diskSpaceResult?.TotalFreeSpace.GetValue(SizeUnit.Bytes), dfEx.Message);
                }
            }

            NumberOfAllocatedPages = _totalAllocationSize / Constants.Storage.PageSize;

            NativeMemory.RegisterFileMapping(FileName.FullPath, new IntPtr(baseAddress), _totalAllocationSize, GetAllocatedInBytes);

            var allocationInfo = new PagerState.AllocationInfo
            {
                BaseAddress = (byte*)baseAddress,
                Size = _totalAllocationSize,
                MappedFile = null
            };
            
            var pager = new PagerState(this, Options.PrefetchSegmentSize, Options.PrefetchResetThreshold, allocationInfo);
            SetPagerState(pager);
        }

        public override byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            var state = pagerState ?? _pagerState;

            if (SysInfo.CanPrefetch)
            {
                if (_pagerState.ShouldPrefetchSegment(pageNumber, out var virtualAddress, out var bytes))
                    rvn_prefetch_virtual_memory(virtualAddress, bytes, out _); // ignore if unsuccessful
            }

            return base.AcquirePagePointer(tx, pageNumber, state);
        }

        public override void Sync(long totalUnsynced)
        {
            var currentState = GetPagerStateAndAddRefAtomically();
            try
            {
                using (var metric = Options.IoMetrics.MeterIoRate(FileName.FullPath, IoMetrics.MeterType.DataSync, 0))
                {
                    foreach (var alloc in currentState.AllocationInfos)
                    {
                        metric.IncrementFileSize(alloc.Size);
                        if (rvn_memory_sync(alloc.BaseAddress, alloc.Size, out var errorCode) != 0)
                            PalHelper.ThrowLastError(errorCode,$"Failed to memory sync at ${new IntPtr(alloc.BaseAddress).ToInt64():X}. TotalUnsynced = ${totalUnsynced}");
                    }
                    metric.IncrementSize(totalUnsynced);
                }
            }
            finally
            {
                currentState.Release();
            }
        }

        public override void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            base.ReleaseAllocationInfo(baseAddress, size);

            var rc = rvn_unmap(baseAddress, size, DeleteOnClose, out var errorCode);
            if (rc != 0)
                PalHelper.ThrowLastError(errorCode,
                    $"Failed to unmap {FileName.FullPath}. DeleteOnClose={DeleteOnClose}");

            NativeMemory.UnregisterFileMapping(FileName?.FullPath, new IntPtr(baseAddress), size);
        }

        protected override void DisposeInternal()
        {
            var rc = rvn_dispose_handle(FileName.FullPath, _handle, DeleteOnClose, out var errorCode);
            if (rc == 0) 
                return;
            
            // ignore results.. but warn! we cannot delete the file probably
            // rc contains either FAIL_UNLINK, FAIL_CLOSE or both of them    (or FAIL_INVALID_HANDLE)
            if (_logger.IsInfoEnabled)
                _logger.Info($"Unable to dispose handle for {FileName.FullPath} (ignoring). rc={(FailCodes)rc}. DeleteOnClose={DeleteOnClose}, "
                             + $"errorCodeClose={PalHelper.GetNativeErrorString(errorCode, "Unable to dispose handle for {FileName.FullPath} (ignoring).", out _)}",
                    new IOException($"Unable to dispose handle for {FileName.FullPath} (ignoring)."));
        }

        protected internal override void PrefetchRanges(Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY* list, int count)
        {
            // TODO : Get rid of WIN32_MEMORY_RANGE_ENTRY and use Pal's PrefetchRanges instead
            rvn_prefetch_ranges((PalDefinitions.PrefetchRanges*)list, count, out _);
            // we explicitly ignore the return code here, this is optimization only
        }

        protected internal override PagerState AllocateMorePages(long newLength)
        {
            if (DisposeOnceRunner.Disposed)
                ThrowAlreadyDisposedException();

            var rc = rvn_allocate_more_space(FileName.FullPath, newLength, _totalAllocationSize, _handle, _copyOnWriteMode ? MmapOptions.CopyOnWrite : MmapOptions.None,
                out var newAddress, out var newLengthAfterAdjustment, out var errorCode);

            if (rc == (int)FailCodes.FailAllocationNoResize)
                return null;

            if (rc != 0)
                PalHelper.ThrowLastError(errorCode, $"can't allocate more pages (rc={rc})");

            // TODO : Get rid of allocation info
            var allocationInfo = new PagerState.AllocationInfo
            {
                BaseAddress = (byte*)newAddress,
                Size = newLengthAfterAdjustment,
                MappedFile = null
            };

            var newPagerState = new PagerState(this, Options.PrefetchSegmentSize, Options.PrefetchResetThreshold, allocationInfo);

            newPagerState.DebugVerify(newLengthAfterAdjustment);

            newPagerState.CopyPrefetchState(_pagerState);
            SetPagerState(newPagerState);

            _totalAllocationSize = newLengthAfterAdjustment;
            NumberOfAllocatedPages = _totalAllocationSize / Constants.Storage.PageSize;

            return newPagerState;
        }

        internal override void ProtectPageRange(byte* start, ulong size, bool force = false)
        {
            if (size == 0 || 
                UsePageProtection == false && force == false) 
                return;

            if (rvn_protect_range(start, (long)size, true, out var errorCode) == 0)
                return;

            if (_logger.IsInfoEnabled)
                _logger.Info($"Unable to protect page range. start={new IntPtr(start).ToInt64():X}, size={size}, PROT_READ, errorCode={errorCode}");
            Debugger.Break();
        }

        internal override void UnprotectPageRange(byte* start, ulong size, bool force = false)
        {
            if (size == 0 || 
                UsePageProtection == false && force == false) 
                return;

            if (rvn_protect_range(start, (long)size, false, out var errorCode) == 0)
                return;

            if (_logger.IsInfoEnabled)
                _logger.Info($"Unable to un-protect page range. start={new IntPtr(start).ToInt64():X}, size={size}, PROT_READ | PROT_WRITE, errorCode={errorCode}");
            Debugger.Break();
        }
    }
}
