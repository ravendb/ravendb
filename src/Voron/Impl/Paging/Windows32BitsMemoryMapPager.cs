using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Utils;
using Voron.Data;
using Voron.Global;
using Voron.Platform.Win32;
using Voron.Util.Settings;
using static Voron.Platform.Win32.Win32MemoryMapNativeMethods;
using static Voron.Platform.Win32.Win32NativeFileMethods;


namespace Voron.Impl.Paging
{
    public class TransactionState
    {
        public Dictionary<long, LoadedPage> LoadedPages = new Dictionary<long, LoadedPage>();
        public List<MappedAddresses> AddressesToUnload = new List<MappedAddresses>();
        public long TotalLoadedSize;
    }

    public class MappedAddresses
    {
        public string File;
        public IntPtr Address;
        public long StartPage;
        public long Size;
        public int Usages;
    }

    public unsafe class LoadedPage
    {
        public byte* Pointer;
        public int NumberOfPages;
        public long StartPage;
    }

    public sealed unsafe class Windows32BitsMemoryMapPager : AbstractPager
    {
        private readonly Win32NativeFileAttributes _fileAttributes;
        private readonly ConcurrentDictionary<long, ConcurrentSet<MappedAddresses>> _globalMapping = new ConcurrentDictionary<long, ConcurrentSet<MappedAddresses>>(NumericEqualityComparer.BoxedInstanceInt64);
        private long _totalMapped;
        private int _concurrentTransactions;
        private readonly ReaderWriterLockSlim _globalMemory = new ReaderWriterLockSlim();

        public const int AllocationGranularity = 64 * Constants.Size.Kilobyte;
        private const int NumberOfPagesInAllocationGranularity = AllocationGranularity / Constants.Storage.PageSize;
        private readonly FileInfo _fileInfo;
        private readonly FileStream _fileStream;
        private readonly SafeFileHandle _handle;
        private readonly MemoryMappedFileAccess _memoryMappedFileAccess;
        private readonly NativeFileMapAccessType _mmFileAccessType;

        private long _totalAllocationSize;
        private IntPtr _hFileMappingObject;
        private long _fileStreamLength;

        public Windows32BitsMemoryMapPager(StorageEnvironmentOptions options, VoronPathSetting file, long? initialFileSize = null,
            Win32NativeFileAttributes fileAttributes = Win32NativeFileAttributes.Normal,
            Win32NativeFileAccess access = Win32NativeFileAccess.GenericRead | Win32NativeFileAccess.GenericWrite,
            bool usePageProtection = false)
            : base(options, canPrefetchAhead: false, usePageProtection: usePageProtection)
        {
            _memoryMappedFileAccess = access == Win32NativeFileAccess.GenericRead
              ? MemoryMappedFileAccess.Read
              : MemoryMappedFileAccess.ReadWrite;

            _mmFileAccessType = access == Win32NativeFileAccess.GenericRead
                ? NativeFileMapAccessType.Read
                : NativeFileMapAccessType.Read |
                  NativeFileMapAccessType.Write;

            FileName = file;

            if (Options.CopyOnWriteMode)
                ThrowNotSupportedOption(file.FullPath);

            _fileAttributes = fileAttributes;
            _handle = CreateFile(file.FullPath, access,
                Win32NativeFileShare.Read | Win32NativeFileShare.Write | Win32NativeFileShare.Delete, IntPtr.Zero,
                Win32NativeFileCreationDisposition.OpenAlways,
                fileAttributes, IntPtr.Zero);


            if (_handle.IsInvalid)
            {
                var lastWin32ErrorCode = Marshal.GetLastWin32Error();
                throw new IOException("Failed to open file storage of Windows32BitsMemoryMapPager for " + file,
                    new Win32Exception(lastWin32ErrorCode));
            }

            _fileInfo = new FileInfo(file.FullPath);

            var streamAccessType = access == Win32NativeFileAccess.GenericRead
                 ? FileAccess.Read
                 : FileAccess.ReadWrite;

            _fileStream = SafeFileStream.Create(_handle, streamAccessType);

            _totalAllocationSize = _fileInfo.Length;

            if ((access & Win32NativeFileAccess.GenericWrite) == Win32NativeFileAccess.GenericWrite ||
                (access & Win32NativeFileAccess.GenericAll) == Win32NativeFileAccess.GenericAll ||
                (access & Win32NativeFileAccess.FILE_GENERIC_WRITE) == Win32NativeFileAccess.FILE_GENERIC_WRITE)
            {
                var fileLength = _fileStream.Length;
                if ((fileLength == 0) && initialFileSize.HasValue)
                    fileLength = initialFileSize.Value;

                if ((_fileStream.Length == 0) || (fileLength % AllocationGranularity != 0))
                {
                    fileLength = NearestSizeToAllocationGranularity(fileLength);

                    SetFileLength(_handle, fileLength);
                }
                _totalAllocationSize = fileLength;
            }

            NumberOfAllocatedPages = _totalAllocationSize/Constants.Storage.PageSize;
            SetPagerState(CreatePagerState());
        }

        private static void ThrowNotSupportedOption(string file)
        {
            throw new NotSupportedException(
                "CopyOnWriteMode is currently not supported for 32 bits, error on " +
                file);
        }

        public override long TotalAllocationSize => _totalAllocationSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

            var ammountToMapInBytes = NearestSizeToAllocationGranularity((distanceFromStart + numberOfPages) * Constants.Storage.PageSize);
            MapPages(state, allocationStartPosition, ammountToMapInBytes);
            return true;
        }

        public override int CopyPage(I4KbBatchWrites destI4KbBatchWrites, long pageNumber, PagerState pagerState)
        {
            var distanceFromStart = (pageNumber % NumberOfPagesInAllocationGranularity);
            var allocationStartPosition = pageNumber - distanceFromStart;

            var offset = new WindowsMemoryMapPager.SplitValue { Value = (ulong)allocationStartPosition * (ulong)Constants.Storage.PageSize };
            var result = MapViewOfFileEx(_hFileMappingObject, _mmFileAccessType, offset.High,
                offset.Low,
                (UIntPtr)AllocationGranularity, null);
            try
            {

                if (result == null)
                {
                    var lastWin32Error = Marshal.GetLastWin32Error();
                    throw new Win32Exception($"Unable to map (default view size) {AllocationGranularity / Constants.Size.Kilobyte:#,#0} kb for page {pageNumber} starting at {allocationStartPosition} on {FileName}",
                        new Win32Exception(lastWin32Error));
                }

                var pageHeader = (PageHeader*)(result + distanceFromStart * Constants.Storage.PageSize);

                int numberOfPages = 1;
                if ((pageHeader->Flags & PageFlags.Overflow) == PageFlags.Overflow)
                {
                    numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(pageHeader->OverflowSize);
                }

                if (numberOfPages + distanceFromStart > NumberOfPagesInAllocationGranularity)
                {
                    UnmapViewOfFile(result);
                    result = null;

                    var newSize = NearestSizeToAllocationGranularity((numberOfPages + distanceFromStart) * Constants.Storage.PageSize);
                    result = MapViewOfFileEx(_hFileMappingObject, _mmFileAccessType, offset.High,
                        offset.Low,
                        (UIntPtr)newSize, null);

                    if (result == null)
                    {
                        var lastWin32Error = Marshal.GetLastWin32Error();
                        throw new Win32Exception($"Unable to map {newSize / Constants.Size.Kilobyte:#,#0} kb for page {pageNumber} starting at {allocationStartPosition} on {FileName}",
                            new Win32Exception(lastWin32Error));
                    }

                    pageHeader = (PageHeader*)(result + (distanceFromStart * Constants.Storage.PageSize));
                }
                const int adjustPageSize = (Constants.Storage.PageSize) / (4 * Constants.Size.Kilobyte);

                destI4KbBatchWrites.Write(pageHeader->PageNumber * adjustPageSize, numberOfPages * adjustPageSize, (byte*)pageHeader);

                return numberOfPages;
            }
            finally
            {
                if (result != null)
                    UnmapViewOfFile(result);
            }
        }

        private void LockMemory32Bits(byte* address, long sizeToLock, TransactionState state)
        {
            try
            {
                if (Sodium.sodium_mlock(address, (UIntPtr)sizeToLock) != 0)
                {
                    if (DoNotConsiderMemoryLockFailureAsCatastrophicError == false)
                    {
                        TryHandleFailureToLockMemory(address, sizeToLock, state.TotalLoadedSize * 2);
                    }
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to lock memory in 32-bit mode", e);
            }
        }

        private void UnlockMemory32Bits(byte* address, long sizeToUnlock)
        {
            try
            {
                Sodium.sodium_munlock(address, (UIntPtr)sizeToUnlock);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to unlock memory in 32-bit mode", e);
            }            
        }

        public override I4KbBatchWrites BatchWriter()
        {
            return new Windows32Bit4KbBatchWrites(this);
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


                var offset = new WindowsMemoryMapPager.SplitValue
                {
                    Value = (ulong)startPage * Constants.Storage.PageSize
                };

                if ((long)offset.Value + size > _fileStreamLength)
                {
                    // this can happen when the file size is not a natural multiple of the allocation granularity
                    // frex: granularity of 64KB, and the file size is 80KB. In this case, a request to map the last
                    // 64 kb will run into a problem, there aren't any. In this case only, we'll map the bytes that are
                    // actually there in the file, and if the codewill attemp to access beyond the end of file, we'll get
                    // an access denied error, but this is already handled in higher level of the code, since we aren't just
                    // handing out access to the full range we are mapping immediately.
                    if ((long)offset.Value < _fileStreamLength)
                        size = _fileStreamLength - (long) offset.Value;
                    else
                        ThrowInvalidMappingRequested(startPage, size);
                }

                var result = MapViewOfFileEx(_hFileMappingObject, _mmFileAccessType, offset.High,
                    offset.Low,
                    (UIntPtr)size, null);

                if (result == null)
                {
                    var lastWin32Error = Marshal.GetLastWin32Error();
                    throw new Win32Exception(
                        $"Unable to map {size / Constants.Size.Kilobyte:#,#0} kb starting at {startPage} on {FileName}",
                        new Win32Exception(lastWin32Error));
                }

                if (_options.EncryptionEnabled && LockMemory)
                    LockMemory32Bits(result, size, state);

                NativeMemory.RegisterFileMapping(_fileInfo.FullName, new IntPtr(result), size, GetAllocatedInBytes);
                Interlocked.Add(ref _totalMapped, size);
                var mappedAddresses = new MappedAddresses
                {
                    Address = (IntPtr)result,
                    File = _fileInfo.FullName,
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

        private void ThrowInvalidMappingRequested(long startPage, long size)
        {
            throw new InvalidOperationException(
                $"Was asked to map page {startPage} + {size / 1024:#,#0} kb, but the file size is only {_fileStreamLength}, can't do that.");
        }

        private TransactionState GetTransactionState(IPagerLevelTransactionState tx)
        {
            if (tx == null)
                throw new NotSupportedException("Cannot use 32 bits pager without a transaction... it's responsible to call unmap");

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

        private PagerState CreatePagerState()
        {
            _fileStreamLength = _fileStream.Length;
            var mmf = MemoryMappedFile.CreateFromFile(_fileStream, null, _fileStreamLength,
               _memoryMappedFileAccess,
                HandleInheritability.None, true);

            var allocation = new PagerState.AllocationInfo
            {
                MappedFile = mmf,
                BaseAddress = null,
                Size = 0
            };

            var newPager = new PagerState(this, Options.PrefetchSegmentSize, Options.PrefetchResetThreshold, allocation);

            _hFileMappingObject = mmf.SafeMemoryMappedFileHandle.DangerousGetHandle();
            return newPager;
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

                    if (_options.EncryptionEnabled && LockMemory)
                        UnlockMemory32Bits((byte*)addr.Address, addr.Size);

                    Interlocked.Add(ref _totalMapped, -addr.Size);
                    UnmapViewOfFile((byte*)addr.Address);
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

        private class Windows32Bit4KbBatchWrites : I4KbBatchWrites
        {
            private readonly Windows32BitsMemoryMapPager _parent;
            private readonly TransactionState _state = new TransactionState();

            public Windows32Bit4KbBatchWrites(Windows32BitsMemoryMapPager parent)
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

                var ammountToMapInBytes = _parent.NearestSizeToAllocationGranularity((distanceFromStart + numberOfPages) * Constants.Storage.PageSize);

                if (_state.LoadedPages.TryGetValue(allocationStartPosition, out var page))
                {
                    if (page.NumberOfPages < distanceFromStart + numberOfPages)
                        page = _parent.MapPages(_state, allocationStartPosition, ammountToMapInBytes);
                }
                else
                    page = _parent.MapPages(_state, allocationStartPosition, ammountToMapInBytes);

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
                    FlushViewOfFile(loadedPage.Pointer, new IntPtr(loadedPage.NumberOfPages * Constants.Storage.PageSize));
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
            if (DisposeOnceRunner.Disposed)
                ThrowAlreadyDisposedException();

            if ((_fileAttributes & Win32NativeFileAttributes.Temporary) == Win32NativeFileAttributes.Temporary ||
                (_fileAttributes & Win32NativeFileAttributes.DeleteOnClose) == Win32NativeFileAttributes.DeleteOnClose)
                return;

            using (var metric = Options.IoMetrics.MeterIoRate(FileName.FullPath, IoMetrics.MeterType.DataSync, 0))
            {
                metric.IncrementSize(totalUnsynced);
                metric.SetFileSize(_totalAllocationSize);

                if (Win32MemoryMapNativeMethods.FlushFileBuffers(_handle) == false)
                {
                    var lastWin32Error = Marshal.GetLastWin32Error();
                    throw new Win32Exception($"Unable to flush file buffers on {FileName}",
                        new Win32Exception(lastWin32Error));
                }
            }
        }

        protected override string GetSourceName()
        {
            if (_fileInfo == null)
                return "Unknown";
            return "MemMap: " + _fileInfo.FullName;
        }

        protected internal override PagerState AllocateMorePages(long newLength)
        {
            var newLengthAfterAdjustment = NearestSizeToAllocationGranularity(newLength);

            if (newLengthAfterAdjustment <= _totalAllocationSize)
                return null;

            var allocationSize = newLengthAfterAdjustment - _totalAllocationSize;

            SetFileLength(_handle, _totalAllocationSize + allocationSize);
            _totalAllocationSize += allocationSize;
            NumberOfAllocatedPages = _totalAllocationSize / Constants.Storage.PageSize;

            var pagerState = CreatePagerState();
            SetPagerState(pagerState);
            return pagerState;
        }

        public override string ToString()
        {
            return _fileInfo.Name;
        }

        protected internal override unsafe void PrefetchRanges(WIN32_MEMORY_RANGE_ENTRY* list, int count)
        {
            // explicitly do nothing here
        }

        protected override void DisposeInternal()
        {
            _fileStream?.Dispose();
            _handle?.Dispose();
            if (DeleteOnClose)
                _fileInfo?.Delete();
        }
    }
}
