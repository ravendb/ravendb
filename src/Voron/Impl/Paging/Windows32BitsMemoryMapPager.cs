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
using Sparrow.Utils;
using Voron.Data;
using Voron.Global;
using Voron.Platform.Win32;
using static Voron.Platform.Win32.Win32MemoryMapNativeMethods;
using static Voron.Platform.Win32.Win32NativeFileMethods;
using static Voron.Platform.Win32.Win32NativeMethods;


namespace Voron.Impl.Paging
{
    public class TransactionState
    {
        public Dictionary<long, LoadedPage> LoadedPages = new Dictionary<long, LoadedPage>();
        public List<MappedAddresses> AddressesToUnload = new List<MappedAddresses>();
    }

    public unsafe class MappedAddresses
    {
        public string File;
        public byte* Address;
        public long Size;
    }

    public unsafe class LoadedPage
    {
        public byte* Pointer;
        public int NumberOfPages;
        public long StartPage;
    }

    public unsafe class Windows32BitsMemoryMapPager : AbstractPager
    {
        public const int AllocationGranularity = 64*Constants.Size.Kilobyte;
        private const int NumberOfPagesInAllocationGranularity = AllocationGranularity / Constants.Storage.PageSize;
        private readonly FileInfo _fileInfo;
        private readonly FileStream _fileStream;
        private readonly SafeFileHandle _handle;
        private readonly MemoryMappedFileAccess _memoryMappedFileAccess;
        private readonly NativeFileMapAccessType _mmFileAccessType;

        private Logger _logger;
        private long _totalAllocationSize;
        private IntPtr _hFileMappingObject;
        private long _fileStreamLength;

        public Windows32BitsMemoryMapPager(StorageEnvironmentOptions options, string file, long? initialFileSize = null,
            Win32NativeFileAttributes fileAttributes = Win32NativeFileAttributes.Normal,
            Win32NativeFileAccess access = Win32NativeFileAccess.GenericRead | Win32NativeFileAccess.GenericWrite,
            bool usePageProtection = false)
            : base(options, usePageProtection)
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
            
            _handle = CreateFile(file, access,
                Win32NativeFileShare.Read | Win32NativeFileShare.Write | Win32NativeFileShare.Delete, IntPtr.Zero,
                Win32NativeFileCreationDisposition.OpenAlways,
                fileAttributes, IntPtr.Zero);


            if (_handle.IsInvalid)
            {
                var lastWin32ErrorCode = Marshal.GetLastWin32Error();
                throw new IOException("Failed to open file storage of Windows32BitsMemoryMapPager for " + file,
                    new Win32Exception(lastWin32ErrorCode));
            }

            _fileInfo = new FileInfo(file);

            var streamAccessType = access == Win32NativeFileAccess.GenericRead
                 ? FileAccess.Read
                 : FileAccess.ReadWrite;

            _fileStream = new FileStream(_handle, streamAccessType);

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


            NumberOfAllocatedPages = _totalAllocationSize / Constants.Storage.PageSize;


            SetPagerState(CreatePagerState());
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

            LoadedPage page;
            if (state.LoadedPages.TryGetValue(allocationStartPosition, out page))
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
            
            if (result == null)
            {
                var lastWin32Error = Marshal.GetLastWin32Error();
                throw new Win32Exception($"Unable to map (default view size) {AllocationGranularity / Constants.Size.Kilobyte:#,#} kb for page {pageNumber} starting at {allocationStartPosition} on {FileName}",
                    new Win32Exception(lastWin32Error));
            }

            var pageHeader = (PageHeader*)(result + distanceFromStart * Constants.Storage.PageSize);

            int numberOfPages = 1;
            if ((pageHeader->Flags & PageFlags.Overflow) == PageFlags.Overflow)
            {
                numberOfPages = this.GetNumberOfOverflowPages(pageHeader->OverflowSize);
            }

            if (numberOfPages + distanceFromStart > NumberOfPagesInAllocationGranularity)
            {
                UnmapViewOfFile(result);

                var newSize = NearestSizeToAllocationGranularity((numberOfPages + distanceFromStart) * Constants.Storage.PageSize);
                result = MapViewOfFileEx(_hFileMappingObject, _mmFileAccessType, offset.High,
                    offset.Low,
                    (UIntPtr)newSize, null);

                if (result == null)
                {
                    var lastWin32Error = Marshal.GetLastWin32Error();
                    throw new Win32Exception($"Unable to map {newSize / Constants.Size.Kilobyte:#,#} kb for page {pageNumber} starting at {allocationStartPosition} on {FileName}",
                        new Win32Exception(lastWin32Error));
                }

                pageHeader = (PageHeader*)(result + (distanceFromStart * Constants.Storage.PageSize));
            }
            const int adjustPageSize = (Constants.Storage.PageSize) / (4 * Constants.Size.Kilobyte);

            destI4KbBatchWrites.Write(pageHeader->PageNumber * adjustPageSize, numberOfPages * adjustPageSize, (byte*)pageHeader);

            UnmapViewOfFile(result);

            return numberOfPages;
        }

        public override I4KbBatchWrites BatchWriter()
        {
            return new Windows32Bit4KbBatchWrites(this);
        }

        public override byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            if (Disposed)
                ThrowAlreadyDisposedException();

            if (pageNumber > NumberOfAllocatedPages || pageNumber < 0)
                ThrowOnInvalidPageNumber(pageNumber, tx.Environment);

            var state = GetTransactionState(tx);

            var distanceFromStart = (pageNumber % NumberOfPagesInAllocationGranularity);
            var allocationStartPosition = pageNumber - distanceFromStart;

            LoadedPage page;
            if (state.LoadedPages.TryGetValue(allocationStartPosition, out page))
            {
                return page.Pointer + (distanceFromStart * Constants.Storage.PageSize);
            }

            page = MapPages(state, allocationStartPosition, AllocationGranularity);
            return page.Pointer + (distanceFromStart*Constants.Storage.PageSize);
        }

        private LoadedPage MapPages(TransactionState state, long startPage, long size)
        {
            var offset = new WindowsMemoryMapPager.SplitValue { Value = (ulong)startPage * (ulong)Constants.Storage.PageSize };

            if ((long)offset.Value + size > _fileStreamLength)
            {
                ThrowInvalidMappingRequested(startPage, size);
            }

            var result = MapViewOfFileEx(_hFileMappingObject, _mmFileAccessType, offset.High,
                offset.Low,
                (UIntPtr)size, null);

            if (result == null)
            {
                var lastWin32Error = Marshal.GetLastWin32Error();
                throw new Win32Exception($"Unable to map {size/Constants.Size.Kilobyte:#,#} kb starting at {startPage} on {FileName}", 
                    new Win32Exception(lastWin32Error));
            }

            NativeMemory.RegisterFileMapping(_fileInfo.FullName, new IntPtr(result), size);

            state.AddressesToUnload.Add(new MappedAddresses
            {
                Address = result,
                File = _fileInfo.FullName,
                Size = size
            });
            var loadedPage = new LoadedPage
            {
                Pointer = result,
                NumberOfPages = (int)(size / Constants.Storage.PageSize),
                StartPage = startPage
            };
            state.LoadedPages[startPage] = loadedPage;
            return loadedPage;
        }

        private void ThrowInvalidMappingRequested(long startPage, long size)
        {
            throw new InvalidOperationException(
                $"Was asked to map page {startPage} + {size/1024:#,#} kb, but the file size is only {_fileStreamLength}, can't do that.");
        }

        private TransactionState GetTransactionState(IPagerLevelTransactionState tx)
        {
            TransactionState transactionState;
            if (tx.Windows32BitsPagerTransactionState == null)
            {
                transactionState = new TransactionState();
                tx.Windows32BitsPagerTransactionState = new Dictionary<AbstractPager, TransactionState>
                {
                    {this, transactionState}
                };
                tx.OnDispose += TxOnOnDispose;
                return transactionState;
            }

            if (tx.Windows32BitsPagerTransactionState.TryGetValue(this, out transactionState) == false)
            {
                transactionState = new TransactionState();
                tx.Windows32BitsPagerTransactionState[this] = transactionState;
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
                Files = new[] { mmf },
                MapBase = null,
                AllocationInfos = new PagerState.AllocationInfo[0]
            };
            _hFileMappingObject = mmf.SafeMemoryMappedFileHandle.DangerousGetHandle();
            return newPager;
        }

        private void TxOnOnDispose(IPagerLevelTransactionState lowLevelTransaction)
        {
            if (lowLevelTransaction.Windows32BitsPagerTransactionState == null)
                return;
            foreach (var state in lowLevelTransaction.Windows32BitsPagerTransactionState.Values)
            {
                foreach (var addr in state.AddressesToUnload)
                {
                    UnmapViewOfFile(addr.Address);
                    NativeMemory.UnregisterFileMapping(addr.File, (IntPtr)addr.Address, addr.Size);
                }
            }
            lowLevelTransaction.Windows32BitsPagerTransactionState.Clear();
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

                LoadedPage page;
                if (_state.LoadedPages.TryGetValue(allocationStartPosition, out page))
                {
                    if (page.NumberOfPages < distanceFromStart + numberOfPages)
                    {
                        UnmapViewOfFile(page.Pointer);
                        for (int i = 0; i < _state.AddressesToUnload.Count; i++)
                        {
                            if (_state.AddressesToUnload[i].Address == page.Pointer)
                            {
                                NativeMemory.UnregisterFileMapping(_state.AddressesToUnload[i].File,
                                    (IntPtr) _state.AddressesToUnload[i].Address, 
                                    _state.AddressesToUnload[i].Size);

                                _state.AddressesToUnload.RemoveAt(i);
                                break;
                            }
                        }
                        page = _parent.MapPages(_state, allocationStartPosition, ammountToMapInBytes);
                    }
                }
                else
                {
                    page = _parent.MapPages(_state, allocationStartPosition, ammountToMapInBytes);
                }

                var toWrite = numberOf4Kbs * 4 * Constants.Size.Kilobyte;
                byte* destination = page.Pointer +
                                    (distanceFromStart*Constants.Storage.PageSize) +
                                    offsetBy4Kb*(4*Constants.Size.Kilobyte);

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
                    FlushViewOfFile(loadedPage.Pointer, new IntPtr(loadedPage.NumberOfPages * Constants.Storage.PageSize));
                }

                foreach (var addr in _state.AddressesToUnload)
                {
                    UnmapViewOfFile(addr.Address);
                    NativeMemory.UnregisterFileMapping(addr.File, (IntPtr)addr.Address, addr.Size);
                }
                _state.AddressesToUnload.Clear();
                _state.LoadedPages.Clear();
            }
        }

        public override void Sync(long totalUnsynced)
        {
            if (Win32MemoryMapNativeMethods.FlushFileBuffers(_handle) == false)
            {
                var lastWin32Error = Marshal.GetLastWin32Error();
                throw new Win32Exception($"Unable to flush file buffers on {FileName}",
                    new Win32Exception(lastWin32Error));
            }
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
            NumberOfAllocatedPages = _totalAllocationSize / Constants.Storage.PageSize;

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