using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron.Exceptions;
using Voron.Global;
using Voron.Platform;
using Voron.Util.Settings;
using static Voron.Platform.Pal;
using static Voron.Platform.PalDefinitions;
using static Voron.Platform.PalFlags;

namespace Voron.Impl.Paging
{
    public sealed unsafe class RvnMemoryMapPager : AbstractPager
    {
        public override long TotalAllocationSize => _totalAllocationSize;
        public override int CopyPage(I4KbBatchWrites destwI4KbBatchWrites, long p, PagerState pagerState) => CopyPageImpl(destwI4KbBatchWrites, p, pagerState);
        public override string ToString() => FileName?.FullPath ?? "";
        protected override string GetSourceName() => $"mmf64: {FileName?.FullPath}";
        private long _totalAllocationSize;
        private readonly Logger _logger;
        private readonly SafeMmapHandle _handle;

        public RvnMemoryMapPager(StorageEnvironmentOptions options, VoronPathSetting file, long? initialFileSize = null, bool canPrefetchAhead = true, bool usePageProtection = false, bool deleteOnClose = false)
            : base(options, canPrefetchAhead, usePageProtection)
        {
            DeleteOnClose = deleteOnClose;
            FileName = file;
            var copyOnWriteMode = options.CopyOnWriteMode && FileName.FullPath.EndsWith(Constants.DatabaseFilename);
            _logger = LoggingSource.Instance.GetLogger<StorageEnvironment>($"Pager-{file}");

            if (initialFileSize.HasValue == false || initialFileSize.Value == 0) 
                initialFileSize = Math.Max(SysInfo.PageSize * 16, 64 * 1024);

            if (initialFileSize % SysInfo.PageSize != 0)
                initialFileSize += SysInfo.PageSize - initialFileSize % SysInfo.PageSize;

            Debug.Assert(file != null);

            var mmapOptions = copyOnWriteMode ? MmapOptions.CopyOnWrite : MmapOptions.None;
            if (DeleteOnClose)
                mmapOptions |= MmapOptions.DeleteOnClose;

            var rc = rvn_create_and_mmap64_file(
                file.FullPath,
                initialFileSize.Value,
                mmapOptions,
                out _handle,
                out var baseAddress,
                out _totalAllocationSize,
                out var errorCode);

            if (rc != FailCodes.Success)
            {
                try
                {
                    PalHelper.ThrowLastError(rc, errorCode, $"rvn_create_and_mmap64_file failed on {rc} for '{file.FullPath}'");
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
                        var rc = rvn_memory_sync(alloc.BaseAddress, alloc.Size, out var errorCode);
                        if (rc != FailCodes.Success)
                            PalHelper.ThrowLastError(rc, errorCode,$"Failed to memory sync at ${new IntPtr(alloc.BaseAddress).ToInt64():X} for for '{FileName.FullPath}'. TotalUnsynced = ${totalUnsynced}");
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

            var rc = rvn_unmap(DeleteOnClose ? MmapOptions.DeleteOnClose : MmapOptions.None,
                baseAddress, size, out var errorCode);
            if (rc != FailCodes.Success)
                PalHelper.ThrowLastError(rc, errorCode,
                    $"Failed to unmap {FileName.FullPath}. DeleteOnClose={DeleteOnClose}");

            NativeMemory.UnregisterFileMapping(FileName?.FullPath, new IntPtr(baseAddress), size);
        }

        protected override void DisposeInternal()
        {
            _handle.Dispose();
            // _handle.FailCode != success, we cannot delete the file probably, and there's nothing much we can do here.
            // just add to log and continue            
            if (_handle.FailCode == FailCodes.Success)
                return;

            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Unable to dispose handle for {FileName.FullPath} (ignoring). rc={_handle.FailCode}. DeleteOnClose={DeleteOnClose}, "
                             + $"errorCode={PalHelper.GetNativeErrorString(_handle.ErrorNo, "Unable to dispose handle for {FileName.FullPath} (ignoring).", out _)}",
                    new IOException($"Unable to dispose handle for {FileName.FullPath} (ignoring)."));
            }
        }

        protected internal override void PrefetchRanges(PrefetchRanges* list, int count)
        {
            rvn_prefetch_ranges(list, count, out _);
            // we explicitly ignore the return code here, this is optimization only
        }


        private long NearestSizeToPageSize(long size)
        {
            // return OS page aligned size, but not less then AllocationGranularity

            Debug.Assert(SysInfo.PageSize > 0);
            Debug.Assert(AllocationGranularity % SysInfo.PageSize == 0);

            if (size == 0)
                return AllocationGranularity;

            if (size % SysInfo.PageSize == 0)
                return Math.Max(size, AllocationGranularity);

            return Math.Max((size / SysInfo.PageSize + 1) * SysInfo.PageSize, AllocationGranularity);
        }

        protected internal override PagerState AllocateMorePages(long newLength)
        {
            if (DisposeOnceRunner.Disposed)
                ThrowAlreadyDisposedException();

            var newLengthAfterAdjustment = NearestSizeToPageSize(newLength);

            if (newLengthAfterAdjustment <= _totalAllocationSize)
                return null;

            var rc = rvn_allocate_more_space(newLengthAfterAdjustment, _handle, out var newAddress, out var errorCode);

            if (rc != FailCodes.Success)
                PalHelper.ThrowLastError(rc, errorCode, $"can't allocate more pages (rc={rc}) for '{FileName.FullPath}'. Requested {newLength} (adjusted to {newLengthAfterAdjustment})");

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

            if (rvn_protect_range(start, (long)size, ProtectRange.Protect, out var errorCode) == 0)
                return;

            if (_logger.IsInfoEnabled)
                _logger.Info($"Unable to protect page range for '{FileName.FullPath}'. start={new IntPtr(start).ToInt64():X}, size={size}, ProtectRange = Protect, errorCode={errorCode}");
        }

        internal override void UnprotectPageRange(byte* start, ulong size, bool force = false)
        {
            if (size == 0 || 
                UsePageProtection == false && force == false) 
                return;

            if (rvn_protect_range(start, (long)size, ProtectRange.Unprotect, out var errorCode) == 0)
                return;

            if (_logger.IsInfoEnabled)
                _logger.Info($"Unable to un-protect page range for '{FileName.FullPath}'. start={new IntPtr(start).ToInt64():X}, size={size}, ProtectRange = Unprotect, errorCode={errorCode}");
        }
    }

    public class SafeMmapHandle : SafeHandle
    {
        public FailCodes FailCode;
        public int ErrorNo;

        public SafeMmapHandle() : base(IntPtr.Zero, true)
        {
        }

        protected override bool ReleaseHandle()
        {
            FailCode = rvn_mmap_dispose_handle(handle, out ErrorNo);

            handle = IntPtr.Zero;
            return FailCode == FailCodes.Success;
        }

        public override bool IsInvalid => handle == IntPtr.Zero;
    }
}
