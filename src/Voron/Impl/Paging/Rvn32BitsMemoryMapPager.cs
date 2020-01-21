using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Server.Exceptions;
using Sparrow.Server.Meters;
using Sparrow.Server.Platform;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Voron.Data;
using Voron.Global;
using Voron.Util.Settings;
using static Sparrow.Server.Platform.Pal;
using static Sparrow.Server.Platform.PalDefinitions;
using static Sparrow.Server.Platform.PalFlags;
namespace Voron.Impl.Paging
{
    public sealed unsafe class Rvn32BitsMemoryMapPager : AbstractPager
    {
        private long _totalAllocationSize;
        public override long TotalAllocationSize => _totalAllocationSize;
        private readonly bool _copyOnWriteMode;
        private readonly SafeMmapHandle _handle;
        private int _concurrentTransactions;
        private readonly ReaderWriterLockSlim _globalMemory = new ReaderWriterLockSlim();
        private readonly ConcurrentDictionary<long, ConcurrentSet<MappedAddresses>> _globalMapping = new ConcurrentDictionary<long, ConcurrentSet<MappedAddresses>>(NumericEqualityComparer.BoxedInstanceInt64);
        private long _totalMapped;
        private readonly string _instanceGuid = Guid.NewGuid().ToString();
        private const int NumberOfPagesInAllocationGranularity = AllocationGranularity / Constants.Storage.PageSize;

        public Rvn32BitsMemoryMapPager(StorageEnvironmentOptions options, VoronPathSetting file, long? initialFileSize = null,
            bool usePageProtection = false) : base(options, canPrefetchAhead: false, usePageProtection: usePageProtection)
        {
            _copyOnWriteMode = options.CopyOnWriteMode && file.FullPath.EndsWith(Constants.DatabaseFilename);
            FileName = file;

            if (Options.CopyOnWriteMode)
                ThrowNotSupportedOption(file.FullPath);

            var mmapOptions = _copyOnWriteMode ? MmapOptions.CopyOnWrite : MmapOptions.None;
            if (DeleteOnClose)
                mmapOptions |= MmapOptions.DeleteOnClose;

            if (initialFileSize.HasValue == false)
                initialFileSize = AllocationGranularity;

            var rc = rvn_create_file(
                file.FullPath,
                initialFileSize.Value,
                mmapOptions,
                out _handle,
                out _totalAllocationSize,
                out var errorCode);


            if (rc != FailCodes.Success)
            {
                try
                {
                    PalHelper.ThrowLastError(rc, errorCode, $"rvn_create_file failed on {rc} for '{file.FullPath}'");
                }
                catch (DiskFullException dfEx)
                {
                    var diskSpaceResult = DiskSpaceChecker.GetDiskSpaceInfo(file.FullPath);
                    throw new DiskFullException(file.FullPath, initialFileSize.Value, diskSpaceResult?.TotalFreeSpace.GetValue(SizeUnit.Bytes), dfEx.Message);
                }
            }

            _handle.Use64BitSemantics = false;

            NumberOfAllocatedPages = _totalAllocationSize / Constants.Storage.PageSize;
            SetPagerState(new PagerState(this, Options.PrefetchSegmentSize, Options.PrefetchResetThreshold));
        }

        private static void ThrowNotSupportedOption(string file)
        {
            throw new NotSupportedException(
                "CopyOnWriteMode is currently not supported for 32 bits, error on " +
                file);
        }

        private long NearestSizeToAllocationGranularity(long size)
        {
            var modulos = size % AllocationGranularity;
            if (modulos == 0)
                return Math.Max(size, AllocationGranularity);

            return (size / AllocationGranularity + 1) * AllocationGranularity;
        }

        public override bool EnsureMapped(IPagerLevelTransactionState tx, long pageNumber, int numberOfPages)
        {
            var distanceFromStart = (pageNumber % NumberOfPagesInAllocationGranularity);

            var allocationStartPosition = pageNumber - distanceFromStart;

            var state = GetTransactionState(tx);

            if (state.LoadedPages.TryGetValue(allocationStartPosition, out var page))
            {
                if (distanceFromStart + numberOfPages < page.NumberOfPages)
                    return false; // already mapped large enough here
            }

            var amountToMapInBytes = NearestSizeToAllocationGranularity((distanceFromStart + numberOfPages) * Constants.Storage.PageSize);

            //precaution, should never throw because PAL should enlarge the file to handle misalignment to granularity
            if(_totalAllocationSize > amountToMapInBytes)
                throw new InvalidOperationException(
                    $@"Tried to map more than the file size. 
                        This shouldn't happen as PAL layer is supposed to handle such cases. 
                            This is probably a bug. (pager file size is {_totalAllocationSize}, but tried to map {amountToMapInBytes} bytes)");

            MapPages(state, allocationStartPosition, amountToMapInBytes);
            return true;
        }

        public override int CopyPage(I4KbBatchWrites destI4KbBatchWrites, long pageNumber, PagerState pagerState)
        {
            var distanceFromStart = (pageNumber % NumberOfPagesInAllocationGranularity);
            var allocationStartPosition = pageNumber - distanceFromStart;
            var offset = allocationStartPosition * Constants.Storage.PageSize;
            var mmapOptions = _copyOnWriteMode ? MmapOptions.CopyOnWrite : MmapOptions.None;

            var rc = rvn_mmap_file(
                AllocationGranularity,
                mmapOptions,
                _handle,
                offset,
                out var baseAddress,
                out var errorCode);

            if (rc != FailCodes.Success)
            {
                try
                {
                    PalHelper.ThrowLastError(rc, errorCode, $"rvn_mmap_file failed on {rc} for '{FileName.FullPath}' - Unable to map (default view size) {AllocationGranularity / Constants.Size.Kilobyte:#,#0} kb for page {pageNumber} starting at {allocationStartPosition}");
                }
                catch (DiskFullException dfEx)
                {
                    var diskSpaceResult = DiskSpaceChecker.GetDiskSpaceInfo(FileName.FullPath);
                    throw new DiskFullException(FileName.FullPath, AllocationGranularity, diskSpaceResult?.TotalFreeSpace.GetValue(SizeUnit.Bytes), dfEx.Message);
                }
            }

            void* newAddress = new IntPtr(-1).ToPointer();
            try
            {
                var pageHeader = (PageHeader*)((byte *)baseAddress + distanceFromStart * Constants.Storage.PageSize);

                int numberOfPages = 1;
                if ((pageHeader->Flags & PageFlags.Overflow) == PageFlags.Overflow)
                {
                    numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize);
                }


                if (numberOfPages + distanceFromStart > NumberOfPagesInAllocationGranularity)
                {
                    var sizeToMap = (numberOfPages + distanceFromStart) * Constants.Storage.PageSize;
                    rc = rvn_remap(baseAddress, out newAddress, _handle, sizeToMap, mmapOptions, offset, out var errorCodeRemap);
                    if (rc != FailCodes.Success)
                    {
                        try
                        {
                            PalHelper.ThrowLastError(rc, errorCodeRemap, $"Unable to re-map [32 Bits] {sizeToMap / Constants.Size.Kilobyte:#,#0} kb for page {pageNumber} starting at {new IntPtr(baseAddress).ToInt64():X} on {FileName}");
                        }
                        catch (DiskFullException dfEx)
                        {
                            var diskSpaceResult = DiskSpaceChecker.GetDiskSpaceInfo(FileName.FullPath);
                            throw new DiskFullException(FileName.FullPath, AllocationGranularity, diskSpaceResult?.TotalFreeSpace.GetValue(SizeUnit.Bytes), dfEx.Message);
                        }
                    }
                    pageHeader = (PageHeader*)((byte *)newAddress + (distanceFromStart * Constants.Storage.PageSize));
                }
                const int adjustPageSize = (Constants.Storage.PageSize) / (4 * Constants.Size.Kilobyte);

                destI4KbBatchWrites.Write(pageHeader->PageNumber * adjustPageSize, numberOfPages * adjustPageSize,
                    (byte*)pageHeader);

                return numberOfPages;
            }
            finally
            {
                if (new IntPtr(newAddress).ToInt64() != -1)
                {
                    rc = rvn_unmap(MmapOptions.None, newAddress, AllocationGranularity, out var errorCodeUnmap);
                    if (rc != FailCodes.Success)
                    {
                        try
                        {
                            PalHelper.ThrowLastError(rc, errorCodeUnmap, $"Unable to unmap [32 Bits] {AllocationGranularity / Constants.Size.Kilobyte:#,#0} kb for page {pageNumber} starting at {allocationStartPosition} on {FileName}");
                        }
                        catch (DiskFullException dfEx)
                        {
                            var diskSpaceResult = DiskSpaceChecker.GetDiskSpaceInfo(FileName.FullPath);
                            throw new DiskFullException(FileName.FullPath, AllocationGranularity, diskSpaceResult?.TotalFreeSpace.GetValue(SizeUnit.Bytes), dfEx.Message);
                        }
                    }
                }
            }
        }

        public override I4KbBatchWrites BatchWriter()
        {
            return new Rvn32Bit4KbBatchWrites(this);
        }

        public override void DiscardPages(long pageNumber, int numberOfPages)
        {
            // REVIEW: Nothing to do here?.
        }

        public override void DiscardWholeFile()
        {
            // REVIEW: Nothing to do here?.
        }

        public override byte* AcquirePagePointerForNewPage(IPagerLevelTransactionState tx, long pageNumber, int numberOfPages, PagerState pagerState = null)
        {
            return AcquirePagePointer(tx, pageNumber, pagerState);
        }

        public override byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            if (DisposeOnceRunner.Disposed)
                ThrowAlreadyDisposedException();

            if (pageNumber > NumberOfAllocatedPages || pageNumber < 0)
                ThrowOnInvalidPageNumber(pageNumber);

            var state = GetTransactionState(tx);

            var distanceFromStart = (pageNumber % NumberOfPagesInAllocationGranularity);
            var allocationStartPosition = pageNumber - distanceFromStart;

            if (state.LoadedPages.TryGetValue(allocationStartPosition, out var page))
                return page.Pointer + (distanceFromStart * Constants.Storage.PageSize);

            page = MapPages(state, allocationStartPosition, AllocationGranularity);
            return page.Pointer + (distanceFromStart * Constants.Storage.PageSize);
        }

        private LoadedPage MapPages(TransactionState state, long startPage, long size)
        {
            _globalMemory.EnterReadLock();
            try
            {
                var addresses = _globalMapping.GetOrAdd(startPage,
                    _ => new ConcurrentSet<MappedAddresses>());
                foreach (var addr in addresses)
                {
                    if (addr.Size < size)
                        continue;

                    Interlocked.Increment(ref addr.Usages);
                    return AddMappingToTransaction(state, startPage, size, addr);
                }

                var offset = startPage * Constants.Storage.PageSize;

                if (offset + size > _totalAllocationSize)
                {
                    throw new InvalidOperationException(
                        $"Was asked to map page {startPage} + {size / 1024:#,#0} kb, but the file size is only {_totalAllocationSize}, can't do that.");
                }


                var rc = rvn_mmap_file(size, _copyOnWriteMode ? MmapOptions.CopyOnWrite : MmapOptions.None, _handle, offset, out var startingBaseAddressPtr,
                    out var errorCode);

                if (rc != FailCodes.Success)
                {
                    try
                    {
                        PalHelper.ThrowLastError(rc, errorCode, $"Unable to map {size / Constants.Size.Kilobyte:#,#0} kb starting at {startPage} on {FileName}");
                    }
                    catch (DiskFullException dfEx)
                    {
                        var diskSpaceResult = DiskSpaceChecker.GetDiskSpaceInfo(FileName.FullPath);
                        throw new DiskFullException(FileName.FullPath, size, diskSpaceResult?.TotalFreeSpace.GetValue(SizeUnit.Bytes), dfEx.Message);
                    }
                }


                var startingBaseAddressIntPtr = new IntPtr(startingBaseAddressPtr);
                NativeMemory.RegisterFileMapping(FileName.FullPath, startingBaseAddressIntPtr, size, GetAllocatedInBytes);
                if (File.Exists("/tmp/debug.tmp"))
                {
                    Console.WriteLine(Environment.StackTrace);
                }

                Interlocked.Add(ref _totalMapped, size);
                var mappedAddresses = new MappedAddresses
                {
                    Address = startingBaseAddressIntPtr,
                    File = FileName.FullPath,
                    Size = size,
                    StartPage = startPage,
                    Usages = 1
                };
                addresses.Add(mappedAddresses);
                return AddMappingToTransaction(state, startPage, size, mappedAddresses);
            }
            finally
            {
                _globalMemory.ExitReadLock();
            }
        }

        private LoadedPage AddMappingToTransaction(TransactionState state, long startPage, long size, MappedAddresses mappedAddresses)
        {
            state.TotalLoadedSize += size;
            state.AddressesToUnload.Add(mappedAddresses);
            var loadedPage = new LoadedPage
            {
                Pointer = (byte*)mappedAddresses.Address,
                NumberOfPages = (int)(size / Constants.Storage.PageSize),
                StartPage = startPage
            };
            state.LoadedPages[startPage] = loadedPage;
            return loadedPage;
        }

        private TransactionState GetTransactionState(IPagerLevelTransactionState tx)
        {
            TransactionState transactionState;
            if (tx.PagerTransactionState32Bits == null)
            {
                Interlocked.Increment(ref _concurrentTransactions);
                transactionState = new TransactionState();
                tx.PagerTransactionState32Bits = new Dictionary<AbstractPager, TransactionState>
                {
                    {this, transactionState}
                };
                tx.OnDispose += TxOnOnDispose;
                return transactionState;
            }

            if (tx.PagerTransactionState32Bits.TryGetValue(this, out transactionState) == false)
            {
                Interlocked.Increment(ref _concurrentTransactions);
                transactionState = new TransactionState();
                tx.PagerTransactionState32Bits[this] = transactionState;
                tx.OnDispose += TxOnOnDispose;
            }
            return transactionState;
        }

        private void TxOnOnDispose(IPagerLevelTransactionState lowLevelTransaction)
        {
            if (lowLevelTransaction.PagerTransactionState32Bits == null)
                return;

            if (lowLevelTransaction.PagerTransactionState32Bits.TryGetValue(this, out var value) == false)
                return; // nothing mapped here

            lowLevelTransaction.PagerTransactionState32Bits.Remove(this);

            var canCleanup = false;
            foreach (var addr in value.AddressesToUnload)
            {
                canCleanup |= Interlocked.Decrement(ref addr.Usages) == 0;
            }

            Interlocked.Decrement(ref _concurrentTransactions);

            if (canCleanup == false)
                return;

            CleanupMemory(value);
        }

        private void CleanupMemory(TransactionState txState)
        {
            _globalMemory.EnterWriteLock();
            try
            {
                foreach (var addr in txState.AddressesToUnload)
                {
                    if (addr.Usages != 0)
                        continue;

                    if (!_globalMapping.TryGetValue(addr.StartPage, out var set))
                        continue;

                    if (!set.TryRemove(addr))
                        continue;

                    Interlocked.Add(ref _totalMapped, -addr.Size);
                    var flags = DeleteOnClose ? MmapOptions.DeleteOnClose : MmapOptions.None;

                    var rc = rvn_unmap(flags, addr.Address.ToPointer(), addr.Size, out var errorCode);
                    if (rc != FailCodes.Success)
                    {
                        PalHelper.ThrowLastError(rc, errorCode, $"rvn_unmap failed on {rc} for '{FileName.FullPath}' at {addr.Address}");
                    }

                    NativeMemory.UnregisterFileMapping(addr.File, addr.Address, addr.Size);

                    if (set.Count == 0)
                    {
                        _globalMapping.TryRemove(addr.StartPage, out set);
                    }
                }
            }
            finally
            {
                _globalMemory.ExitWriteLock();
            }
        }

        private class Rvn32Bit4KbBatchWrites : I4KbBatchWrites
        {
            private readonly Rvn32BitsMemoryMapPager _parent;
            private readonly TransactionState _state = new TransactionState();

            public Rvn32Bit4KbBatchWrites(Rvn32BitsMemoryMapPager parent)
            {
                _parent = parent;
            }

            public void Write(long posBy4Kbs, int numberOf4Kbs, byte* source)
            {
                const int pageSizeTo4KbRatio = (Constants.Storage.PageSize / (4 * Constants.Size.Kilobyte));
                var pageNumber = posBy4Kbs / pageSizeTo4KbRatio;
                var offsetBy4Kb = posBy4Kbs % pageSizeTo4KbRatio;
                var numberOfPages = numberOf4Kbs / pageSizeTo4KbRatio;
                if (posBy4Kbs % pageSizeTo4KbRatio != 0 ||
                    numberOf4Kbs % pageSizeTo4KbRatio != 0)
                    numberOfPages++;

                _parent.EnsureContinuous(pageNumber, numberOfPages);

                var distanceFromStart = (pageNumber % NumberOfPagesInAllocationGranularity);
                var allocationStartPosition = pageNumber - distanceFromStart;

                var amountToMapInBytes = _parent.NearestSizeToAllocationGranularity((distanceFromStart + numberOfPages) * Constants.Storage.PageSize);

                if (_state.LoadedPages.TryGetValue(allocationStartPosition, out var page))
                {
                    if (page.NumberOfPages < distanceFromStart + numberOfPages)
                        page = _parent.MapPages(_state, allocationStartPosition, amountToMapInBytes);
                }
                else
                    page = _parent.MapPages(_state, allocationStartPosition, amountToMapInBytes);

                var toWrite = numberOf4Kbs * 4 * Constants.Size.Kilobyte;
                byte* destination = page.Pointer +
                                    (distanceFromStart * Constants.Storage.PageSize) +
                                    offsetBy4Kb * (4 * Constants.Size.Kilobyte);

                _parent.UnprotectPageRange(destination, (ulong)toWrite);

                Memory.Copy(destination, source, toWrite);

                _parent.ProtectPageRange(destination, (ulong)toWrite);
            }

            public void Dispose()
            {
                foreach (var page in _state.LoadedPages)
                {
                    var loadedPage = page.Value;
                    // we have to do this here because we need to verify that this is actually written to disk
                    // afterward, we can call flush on this
                    var rc = rvn_memory_sync(loadedPage.Pointer, (loadedPage.NumberOfPages * Constants.Storage.PageSize), out var errorCode);
                    if (rc != FailCodes.Success)
                    {
                        PalHelper.ThrowLastError(rc, errorCode, $"Failed to msync on {loadedPage}");
                    }
                }

                var canCleanup = false;
                foreach (var addr in _state.AddressesToUnload)
                {
                    canCleanup |= Interlocked.Decrement(ref addr.Usages) == 0;
                }
                if (canCleanup)
                    _parent.CleanupMemory(_state);
            }
        }

        public override void Sync(long totalUnsynced)
        {
            using (var metric = Options.IoMetrics.MeterIoRate(FileName.FullPath, IoMetrics.MeterType.DataSync, 0))
            {
                metric.IncrementSize(totalUnsynced);
                metric.IncrementFileSize(_totalAllocationSize);

                var rc = rvn_flush_file(_handle, out var errorCode);
                if (rc != FailCodes.Success)
                {
                    PalHelper.ThrowLastError(rc, errorCode, $"Unable to sync (flush file) {FileName}");
                }
            }
        }

        protected override string GetSourceName()
        {
            return "mmap: " +  _instanceGuid + " " + FileName;
        }

        protected internal override PagerState AllocateMorePages(long newLength)
        {
            var newLengthAfterAdjustment = NearestSizeToAllocationGranularity(newLength);

            if (newLengthAfterAdjustment <= _totalAllocationSize)
                return null;

            var allocationSize = newLengthAfterAdjustment - _totalAllocationSize;

            var rc = rvn_allocate_more_space(mapAfterAllocationFlag: 0, _totalAllocationSize + allocationSize, _handle, out var _, out var errorCode);
            if (rc != FailCodes.Success)
            {
                PalHelper.ThrowLastError(rc, errorCode, $"Unable to rvn_allocate_more_space {FileName}, new size after adjustment: {_totalAllocationSize + allocationSize}");
            }


            _totalAllocationSize += allocationSize;
            NumberOfAllocatedPages = _totalAllocationSize / Constants.Storage.PageSize;

            return null;
        }

        public override string ToString()
        {
            return FileName.FullPath;
        }

        protected override bool CanPrefetchQuery()
        {
            // explicitly disable prefetch
            return false;
        }

        // public override void ReleaseAllocationInfo(byte* baseAddress, long size) - use base class method (32 bits is not (supporting) unmapping here

        protected override void DisposeInternal()
        {
            lock(_handle)
            {
                if(!_handle.IsInvalid)
                    _handle.Dispose();
            }
        }
    }
}
