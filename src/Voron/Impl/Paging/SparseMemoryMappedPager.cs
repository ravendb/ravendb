using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using Sparrow;
using Sparrow.Logging;
using Voron.Data;
using Voron.Platform.Win32;
using static Voron.Platform.Win32.Win32MemoryMapNativeMethods;
using static Voron.Platform.Win32.Win32NativeFileMethods;
using static Voron.Platform.Win32.Win32NativeMethods;


namespace Voron.Impl.Paging
{
    public unsafe class SparseMemoryMappedPager : AbstractPager
    {

        public class TransactionState
        {
            public Dictionary<long, LoadedPage> LoadedPages = new Dictionary<long, LoadedPage>();
            public List<IntPtr> AddressesToUnload = new List<IntPtr>();
        }

        public class LoadedPage
        {
            public byte* Pointer;
            public int NumberOfPages;
            public long StartPage;
        }

        public readonly long AllocationGranularity;
        private readonly FileInfo _fileInfo;
        private readonly FileStream _fileStream;
        private readonly SafeFileHandle _handle;
        private readonly MemoryMappedFileAccess _memoryMappedFileAccess;
        private readonly NativeFileMapAccessType _mmFileAccessType;

        private Logger _logger;
        private long _totalAllocationSize;
        private IntPtr _hFileMappingObject;
        private long _fileStreamLength;

        public SparseMemoryMappedPager(StorageEnvironmentOptions options, string file, long? initialFileSize = null,
            Win32NativeFileAttributes fileAttributes = Win32NativeFileAttributes.Normal,
            Win32NativeFileAccess access = Win32NativeFileAccess.GenericRead | Win32NativeFileAccess.GenericWrite)
            : base(options)
        {
            _memoryMappedFileAccess = access == Win32NativeFileAccess.GenericRead
              ? MemoryMappedFileAccess.Read
              : MemoryMappedFileAccess.ReadWrite;

            _mmFileAccessType = access == Win32NativeFileAccess.GenericRead
                ? NativeFileMapAccessType.Read
                : NativeFileMapAccessType.Read |
                  NativeFileMapAccessType.Write;

            FileName = file;
            _logger = LoggingSource.Instance.GetLogger<StorageEnvironment>($"Pager-{file}");

            if (Options.CopyOnWriteMode)
                throw new NotImplementedException("CopyOnWriteMode using spare memory is currently not supported on " +
                                                  file);

            SYSTEM_INFO info;
            GetSystemInfo(out info);
            AllocationGranularity = info.allocationGranularity;

            _handle = CreateFile(file, access, 
                Win32NativeFileShare.Read | Win32NativeFileShare.Write | Win32NativeFileShare.Delete, IntPtr.Zero,
                Win32NativeFileCreationDisposition.OpenAlways, 
                fileAttributes, IntPtr.Zero);


            if (_handle.IsInvalid)
            {
                var lastWin32ErrorCode = Marshal.GetLastWin32Error();
                throw new IOException("Failed to open file storage of Win32MemoryMapPager for " + file,
                    new Win32Exception(lastWin32ErrorCode));
            }

            _fileInfo = new FileInfo(file);

            var streamAccessType = access == Win32NativeFileAccess.GenericRead
                 ? FileAccess.Read
                 : FileAccess.ReadWrite;

            _fileStream = new FileStream(_handle, streamAccessType);

            _totalAllocationSize = _fileInfo.Length;

            if (access.HasFlag(Win32NativeFileAccess.GenericWrite) ||
                access.HasFlag(Win32NativeFileAccess.GenericAll) ||
                access.HasFlag(Win32NativeFileAccess.FILE_GENERIC_WRITE))
            {
                var fileLength = _fileStream.Length;
                if ((fileLength == 0) && initialFileSize.HasValue)
                    fileLength = initialFileSize.Value;

                if ((_fileStream.Length == 0) || (fileLength%AllocationGranularity != 0))
                {
                    fileLength = NearestSizeToAllocationGranularity(fileLength);

                    SetFileLength(_handle, fileLength);
                }
                _totalAllocationSize = fileLength;
            }


            NumberOfAllocatedPages = _totalAllocationSize/PageSize;

            SetPagerState(CreatePagerState());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long NearestSizeToAllocationGranularity(long size)
        {
            var modulos = size%AllocationGranularity;
            if (modulos == 0)
                return Math.Max(size, AllocationGranularity);

            return (size/AllocationGranularity + 1)*AllocationGranularity;
        }

        public override void EnsureMapped(IPagerLevelTransactionState tx, long pageNumber, int numberOfPages)
        {
            var distanceFromStart = (pageNumber % 16);

            if (distanceFromStart + numberOfPages <= 16)
                return;

            var allocationStartPosition = pageNumber - distanceFromStart;

            var state = GetTransactionState(tx);

            LoadedPage page;
            if (state.LoadedPages.TryGetValue(pageNumber, out page))
            {
                if (allocationStartPosition + numberOfPages < page.NumberOfPages)
                    return; // already mapped large enough here
            }

            var ammountToMapInBytes = NearestSizeToAllocationGranularity((distanceFromStart + numberOfPages)*PageSize);
            MapPages(state, allocationStartPosition, ammountToMapInBytes);
        }

        public override int CopyPage(IPagerBatchWrites destPagerBatchWrites, long pageNumber, PagerState pagerState)
        {
            var distanceFromStart = (pageNumber % 16);
            var allocationStartPosition = pageNumber - distanceFromStart;
         
            var offset = new Win32MemoryMapPager.SplitValue { Value = (ulong)allocationStartPosition * (ulong)PageSize };
            var result = MapViewOfFileEx(_hFileMappingObject, _mmFileAccessType, offset.High,
                offset.Low,
                (UIntPtr)(16 * PageSize), null);
            if (result == null)
                throw new Win32Exception();

            var pageHeader = (PageHeader*)(result + distanceFromStart * PageSize);

            int numberOfPages = 1;
            if ((pageHeader->Flags & PageFlags.Overflow) == PageFlags.Overflow)
            {
                numberOfPages = this.GetNumberOfOverflowPages(pageHeader->OverflowSize);
            }

            if (numberOfPages + distanceFromStart > 16)
            {
                UnmapViewOfFile(result);
                result = MapViewOfFileEx(_hFileMappingObject, _mmFileAccessType, offset.High,
                    offset.Low,
                    (UIntPtr)(NearestSizeToAllocationGranularity((numberOfPages + distanceFromStart) * PageSize)), null);
                if (result == null)
                    throw new Win32Exception();

                pageHeader = (PageHeader*)(result + (distanceFromStart * PageSize));
            }

            destPagerBatchWrites.Write(pageHeader->PageNumber, numberOfPages, (byte*)pageHeader);

            UnmapViewOfFile(result);

            return numberOfPages;
        }

        public override IPagerBatchWrites BatchWriter()
        {
            return new SparsePagerBatchWrites(this);
        }

        public override byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            if (Disposed)
                ThrowAlreadyDisposedException();

            if (pageNumber > NumberOfAllocatedPages || pageNumber < 0)
                ThrowOnInvalidPageNumber(pageNumber, tx.Environment);

            var state = GetTransactionState(tx);

            var distanceFromStart = (pageNumber % 16);
            var allocationStartPosition = pageNumber - distanceFromStart;

            LoadedPage page;
            if (state.LoadedPages.TryGetValue(allocationStartPosition, out page))
            {
                return ReturnPagePointerOrGrowAllocation(page, distanceFromStart, state, canUnmap: false);
            }

            page = MapPages(state, allocationStartPosition, AllocationGranularity);
            return ReturnPagePointerOrGrowAllocation(page, distanceFromStart, state, canUnmap: true);
        }

        private byte* ReturnPagePointerOrGrowAllocation(LoadedPage page, long distanceFromStart, TransactionState state, bool canUnmap)
        {
            var pageHeader = (PageHeader*) (page.Pointer + (distanceFromStart * PageSize));
            if ((pageHeader->Flags & PageFlags.Overflow) != PageFlags.Overflow)
            {
                // single page, already loaded, can return immediately.
                return (byte*) pageHeader;
            }
            // overflow, so need to make sure it is in the range we mapped.
            var numberOfOverflowPages = this.GetNumberOfOverflowPages(pageHeader->OverflowSize);
            if (numberOfOverflowPages + distanceFromStart < page.NumberOfPages)
            {
                // the entire range is already mapped, can return immediately
                return (byte*) pageHeader;
            }

            if (canUnmap)
            {
                Debug.Assert(state.AddressesToUnload[state.AddressesToUnload.Count-1] == new IntPtr(page.Pointer));
                state.AddressesToUnload.RemoveAt(state.AddressesToUnload.Count-1);

                UnmapViewOfFile(page.Pointer);
            }

            var ammountToMapInBytes = NearestSizeToAllocationGranularity((distanceFromStart + numberOfOverflowPages) * PageSize);
            page = MapPages(state, page.StartPage, ammountToMapInBytes);
            return page.Pointer + distanceFromStart * PageSize;
        }

        private LoadedPage MapPages(TransactionState state, long startPage, long size)
        {
            var offset = new Win32MemoryMapPager.SplitValue {Value = (ulong)startPage * (ulong)PageSize };

            if ((long)offset.Value + size > _fileStreamLength)
            {
                size = _fileStreamLength - (long)offset.Value;
            }

            var result = MapViewOfFileEx(_hFileMappingObject, _mmFileAccessType, offset.High,
                offset.Low,
                (UIntPtr) size, null);

            if (result == null)
                throw new Win32Exception();

            state.AddressesToUnload.Add(new IntPtr(result));
            var loadedPage = new LoadedPage
            {
                Pointer = result,
                NumberOfPages = (int) (size/PageSize),
                StartPage = startPage
            };
            state.LoadedPages[startPage] = loadedPage;
            return loadedPage;
        }

        private TransactionState GetTransactionState(IPagerLevelTransactionState tx)
        {
            TransactionState transactionState;
            if (tx.SparsePagerTransactionState == null)
            {
                transactionState = new TransactionState();
                tx.SparsePagerTransactionState = new Dictionary<AbstractPager, TransactionState>
                {
                    {this, transactionState}
                };
                tx.OnDispose += TxOnOnDispose;
                return transactionState;
            }

            if (tx.SparsePagerTransactionState.TryGetValue(this, out transactionState) == false)
            {
                transactionState = new TransactionState();
                tx.SparsePagerTransactionState[this] = transactionState;
            }
            return transactionState;
        }

        private PagerState CreatePagerState()
        {
            _fileStreamLength = _fileStream.Length;
            var mmf = MemoryMappedFile.CreateFromFile(_fileStream, null, _fileStreamLength,
               _memoryMappedFileAccess,
                HandleInheritability.None, true);

            var newPager = new PagerState(this)
            {
                Files = new[] {mmf},
                MapBase = null,
                AllocationInfos = new PagerState.AllocationInfo[0]
            };
            _hFileMappingObject = mmf.SafeMemoryMappedFileHandle.DangerousGetHandle();
            return newPager;
        }

        private void TxOnOnDispose(IPagerLevelTransactionState lowLevelTransaction)
        {
            if (lowLevelTransaction.SparsePagerTransactionState == null)
                return;
            foreach (var state in lowLevelTransaction.SparsePagerTransactionState.Values)
            {
                foreach (var addr in state.AddressesToUnload)
                {
                    UnmapViewOfFile((byte*) addr);
                }
            }
            lowLevelTransaction.SparsePagerTransactionState.Clear();
        }

        private class SparsePagerBatchWrites : IPagerBatchWrites
        {
            private readonly SparseMemoryMappedPager _parent;
            private readonly TransactionState _state = new TransactionState();

            public SparsePagerBatchWrites(SparseMemoryMappedPager parent)
            {
                _parent = parent;
            }

            public void Write(long pageNumber, int numberOfPages, byte* source)
            {
                var distanceFromStart = (pageNumber % 16);
                var allocationStartPosition = pageNumber - distanceFromStart;

                var ammountToMapInBytes = _parent.NearestSizeToAllocationGranularity((distanceFromStart + numberOfPages)*_parent.PageSize);

                LoadedPage page;
                if (_state.LoadedPages.TryGetValue(allocationStartPosition, out page))
                {
                    if (page.NumberOfPages < distanceFromStart + numberOfPages)
                    {
                        UnmapViewOfFile(page.Pointer);
                        _state.AddressesToUnload.Remove(new IntPtr(page.Pointer));
                        page = _parent.MapPages(_state, allocationStartPosition, ammountToMapInBytes);
                    }
                }
                else
                {
                    page = _parent.MapPages(_state, allocationStartPosition, ammountToMapInBytes);
                }

                var toWrite = numberOfPages * _parent.PageSize;
                byte* destination = page.Pointer + distanceFromStart * _parent.PageSize;

                _parent.UnprotectPageRange(destination, (ulong)toWrite);

                Memory.BulkCopy(destination, source, toWrite);

                _parent.ProtectPageRange(destination, (ulong)toWrite);
            }

            public void Dispose()
            {
                foreach (var page in _state.LoadedPages)
                {
                    var loadedPage = page.Value;
                    // we have to do this here because we need to verify that this is actually written to disk
                    // afterward, we can call flush on this
                    FlushViewOfFile(loadedPage.Pointer, new IntPtr(loadedPage.NumberOfPages * _parent.PageSize));
                }
            
                foreach (var ptr in _state.AddressesToUnload)
                {
                    UnmapViewOfFile((byte*)ptr);
                }
                _state.AddressesToUnload.Clear();
                _state.LoadedPages.Clear();
            }
        }

        public override void Sync()
        {
            if (Win32MemoryMapNativeMethods.FlushFileBuffers(_handle) == false)
                throw new Win32Exception();
        }

        protected override string GetSourceName()
        {
            if (_fileInfo == null)
                return "Unknown";
            return "MemMap: " + _fileInfo.FullName;
        }

        protected override PagerState AllocateMorePages(long newLength)
        {
            var newLengthAfterAdjustment = NearestSizeToAllocationGranularity(newLength);

            if (newLengthAfterAdjustment <= _totalAllocationSize)
                return null;

            var allocationSize = newLengthAfterAdjustment - _totalAllocationSize;

            SetFileLength(_handle, _totalAllocationSize + allocationSize);
            _totalAllocationSize += allocationSize;
            NumberOfAllocatedPages = _totalAllocationSize / PageSize;

            var pagerState = CreatePagerState();
            SetPagerState(pagerState);
            return pagerState;
        }

        public override string ToString()
        {
            return _fileInfo.Name;
        }

        public override void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            // this isn't actually ever called here, we don't have a global 
            // memory map, only per transaction
        }

        public override void TryPrefetchingWholeFile()
        {
            // we never want to do this, we'll rely on the OS to do it for us
        }

        public override void MaybePrefetchMemory(List<long> pagesToPrefetch)
        {
            // we never want to do this here
        }

        public override void Dispose()
        {
            if (Disposed)
                return;

            base.Dispose();

            _fileStream?.Dispose();
            _handle?.Dispose();
            if (DeleteOnClose)
                _fileInfo?.Delete();

        }
    }
}