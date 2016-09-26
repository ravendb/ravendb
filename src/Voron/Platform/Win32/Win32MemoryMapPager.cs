using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using Sparrow;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Platform.Win32
{
    public unsafe class Win32MemoryMapPager : AbstractPager
    {
        public readonly long AllocationGranularity;
        private long _totalAllocationSize;
        private readonly FileInfo _fileInfo;
        private readonly FileStream _fileStream;
        private readonly SafeFileHandle _handle;
        private readonly Win32NativeFileAccess _access;
        private readonly MemoryMappedFileAccess _memoryMappedFileAccess;
        private bool _copyOnWriteMode;

        [StructLayout(LayoutKind.Explicit)]
        private struct SplitValue
        {
            [FieldOffset(0)]
            public ulong Value;

            [FieldOffset(0)]
            public uint Low;

            [FieldOffset(4)]
            public uint High;
        }

        public Win32MemoryMapPager(StorageEnvironmentOptions options,string file,
            long? initialFileSize = null,
                                   Win32NativeFileAttributes fileAttributes = Win32NativeFileAttributes.Normal,
                                   Win32NativeFileAccess access = Win32NativeFileAccess.GenericRead | Win32NativeFileAccess.GenericWrite)
            :base(options)
        {
            Win32NativeMethods.SYSTEM_INFO systemInfo;
            Win32NativeMethods.GetSystemInfo(out systemInfo);
            FileName = file;
            AllocationGranularity = systemInfo.allocationGranularity;
            _access = access;
            _copyOnWriteMode = Options.CopyOnWriteMode && FileName.EndsWith(Constants.DatabaseFilename);
            if (options.CopyOnWriteMode && _copyOnWriteMode)
            {
                _memoryMappedFileAccess = MemoryMappedFileAccess.Read | MemoryMappedFileAccess.CopyOnWrite;
                fileAttributes = Win32NativeFileAttributes.Readonly;
                _access = Win32NativeFileAccess.GenericRead;
            }
            else
            {
                _memoryMappedFileAccess = _access == Win32NativeFileAccess.GenericRead
                ? MemoryMappedFileAccess.Read
                : MemoryMappedFileAccess.ReadWrite;
            }

            _handle = Win32NativeFileMethods.CreateFile(file, access,
                                                        Win32NativeFileShare.Read | Win32NativeFileShare.Write | Win32NativeFileShare.Delete, IntPtr.Zero,
                                                        Win32NativeFileCreationDisposition.OpenAlways, fileAttributes, IntPtr.Zero);
            if (_handle.IsInvalid)
            {
                int lastWin32ErrorCode = Marshal.GetLastWin32Error();
                throw new IOException("Failed to open file storage of Win32MemoryMapPager for " + file,
                    new Win32Exception(lastWin32ErrorCode));
            }

            _fileInfo = new FileInfo(file);

            var streamAccessType = _access == Win32NativeFileAccess.GenericRead
                ? FileAccess.Read
                : FileAccess.ReadWrite;
            _fileStream = new FileStream(_handle, streamAccessType);

            _totalAllocationSize = _fileInfo.Length;

            if (_access.HasFlag(Win32NativeFileAccess.GenericWrite) ||
                _access.HasFlag(Win32NativeFileAccess.GenericAll) ||
                _access.HasFlag(Win32NativeFileAccess.FILE_GENERIC_WRITE))
            {
                var fileLength = _fileStream.Length;
                if (fileLength == 0 && initialFileSize.HasValue)
                    fileLength = initialFileSize.Value;

                if (_fileStream.Length == 0 || (fileLength % AllocationGranularity != 0))
                {
                    fileLength = NearestSizeToAllocationGranularity(fileLength);

                    Win32NativeFileMethods.SetFileLength(_handle, fileLength);
                }

                _totalAllocationSize = fileLength;
            }

            NumberOfAllocatedPages = _totalAllocationSize / _pageSize;
            PagerState.Release();
            PagerState = CreatePagerState();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long NearestSizeToAllocationGranularity(long size)
        {
            var modulos = size % AllocationGranularity;
            if (modulos == 0)
                return Math.Max(size, AllocationGranularity);

            return ((size / AllocationGranularity) + 1) * AllocationGranularity;
        }

        protected override PagerState AllocateMorePages(long newLength)
        {
            if (Disposed)
                ThrowAlreadyDisposedException();

            var newLengthAfterAdjustment = NearestSizeToAllocationGranularity(newLength);

            if (newLengthAfterAdjustment <= _totalAllocationSize)
                return null;

            var allocationSize = newLengthAfterAdjustment - _totalAllocationSize;

            Win32NativeFileMethods.SetFileLength(_handle, _totalAllocationSize + allocationSize);
            PagerState newPagerState = null;
            if (TryAllocateMoreContinuousPages(allocationSize) == false)
            {
                newPagerState = CreatePagerState();

                var tmp = PagerState;
                PagerState = newPagerState;
                tmp.Release(); //replacing the pager state --> so one less reference for it
                PagerState.DebugVerify(newLengthAfterAdjustment);
            }

            _totalAllocationSize += allocationSize;
            NumberOfAllocatedPages = _totalAllocationSize / _pageSize;

            return newPagerState;
        }

        private bool TryAllocateMoreContinuousPages(long allocationSize)
        {
            Debug.Assert(PagerState != null);
            Debug.Assert(PagerState.AllocationInfos != null);
            Debug.Assert(PagerState.Files != null && PagerState.Files.Any());

            var allocationInfo = RemapViewOfFileAtAddress(allocationSize, (ulong)_totalAllocationSize, PagerState.MapBase + _totalAllocationSize);

            if (allocationInfo == null)
                return false;

            PagerState.Files = PagerState.Files.Concat(allocationInfo.MappedFile);
            PagerState.AllocationInfos = PagerState.AllocationInfos.Concat(allocationInfo);

            if (Sparrow.Platform.CanPrefetch)
            {
                // We are asking to allocate pages. It is a good idea that they should be already in memory to only cause a single page fault (as they are continuous).
                Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY entry;
                entry.VirtualAddress = allocationInfo.BaseAddress;
                entry.NumberOfBytes = (IntPtr)allocationInfo.Size;

                Win32MemoryMapNativeMethods.PrefetchVirtualMemory(Win32Helper.CurrentProcess, (UIntPtr)1, &entry, 0);
            }
            return true;
        }

        private PagerState.AllocationInfo RemapViewOfFileAtAddress(long allocationSize, ulong offsetInFile, byte* baseAddress)
        {
            var offset = new SplitValue { Value = offsetInFile };

            var mmf = MemoryMappedFile.CreateFromFile(_fileStream, null, _fileStream.Length,
                _memoryMappedFileAccess,
                 HandleInheritability.None, true);
            Win32MemoryMapNativeMethods.NativeFileMapAccessType mmfAccessType = _copyOnWriteMode
                ? Win32MemoryMapNativeMethods.NativeFileMapAccessType.Copy 
                : Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read |
                  Win32MemoryMapNativeMethods.NativeFileMapAccessType.Write;
            var newMappingBaseAddress = Win32MemoryMapNativeMethods.MapViewOfFileEx(mmf.SafeMemoryMappedFileHandle.DangerousGetHandle(),
                mmfAccessType,
                offset.High, offset.Low,
                new UIntPtr((ulong)allocationSize),
                baseAddress);

            var hasMappingSucceeded = newMappingBaseAddress != null && newMappingBaseAddress != (byte*)0;
            if (!hasMappingSucceeded)
            {
                mmf.Dispose();
                return null;
            }

            return new PagerState.AllocationInfo
            {
                BaseAddress = newMappingBaseAddress,
                Size = allocationSize,
                MappedFile = mmf
            };
        }

        private PagerState CreatePagerState()
        {
            var mmf = MemoryMappedFile.CreateFromFile(_fileStream, null, _fileStream.Length,
                _memoryMappedFileAccess,
                HandleInheritability.None, true);

            var fileMappingHandle = mmf.SafeMemoryMappedFileHandle.DangerousGetHandle();
            Win32MemoryMapNativeMethods.NativeFileMapAccessType mmFileAccessType;
            if (_copyOnWriteMode)
            {
                mmFileAccessType =  Win32MemoryMapNativeMethods.NativeFileMapAccessType.Copy;
            }
            else
            {
                mmFileAccessType = _access == Win32NativeFileAccess.GenericRead
                    ? Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read
                    : Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read |
                      Win32MemoryMapNativeMethods.NativeFileMapAccessType.Write;
            }
            var startingBaseAddressPtr = Win32MemoryMapNativeMethods.MapViewOfFileEx(fileMappingHandle,
                mmFileAccessType,
                0, 0,
                UIntPtr.Zero, //map all what was "reserved" in CreateFileMapping on previous row
                null);


            if (startingBaseAddressPtr == (byte*)0) //system didn't succeed in mapping the address where we wanted
            {
                var innerException = new Win32Exception();

                var errorMessage = string.Format(
                    "Unable to allocate more pages - unsuccessfully tried to allocate continuous block of virtual memory with size = {0:##,###;;0} bytes",
                    (_fileStream.Length));

                throw new OutOfMemoryException(errorMessage, innerException);
            }

            var allocationInfo = new PagerState.AllocationInfo
            {
                BaseAddress = startingBaseAddressPtr,
                Size = _fileStream.Length,
                MappedFile = mmf
            };

            var newPager = new PagerState(this)
            {
                Files = new[] { mmf },
                MapBase = startingBaseAddressPtr,
                AllocationInfos = new[] { allocationInfo }
            };

            newPager.AddRef(); // one for the pager
            return newPager;
        }

        protected override string GetSourceName()
        {
            if (_fileInfo == null)
                return "Unknown";
            return "MemMap: " + _fileInfo.FullName;
        }

        public override void Sync()
        {
            if (Disposed)
                ThrowAlreadyDisposedException();

            long totalSize = 0;
            foreach (var allocationInfo in PagerState.AllocationInfos)
            {
                totalSize += allocationInfo.Size;
            }
            using (Options.IoMetrics.MeterIoRate(FileName,IoMetrics.MeterType.Sync, totalSize))
            {
                foreach (var allocationInfo in PagerState.AllocationInfos)
                {
                    if (
                        Win32MemoryMapNativeMethods.FlushViewOfFile(allocationInfo.BaseAddress,
                            new IntPtr(allocationInfo.Size)) == false)
                        throw new Win32Exception();
                }

                if (Win32MemoryMapNativeMethods.FlushFileBuffers(_handle) == false)
                    throw new Win32Exception();
            }
        }


        public override string ToString()
        {
            return _fileInfo.Name;
        }


        public override void Dispose()
        {
            if (Disposed)
                return;

            _fileStream?.Dispose();
            _handle?.Dispose();
            if (DeleteOnClose)
                _fileInfo?.Delete();

            base.Dispose();
        }

        public override void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            if (Win32MemoryMapNativeMethods.UnmapViewOfFile(baseAddress) == false)
                throw new Win32Exception();
        }

        public override void MaybePrefetchMemory(List<long> pagesToPrefetch)
        {
            if (Sparrow.Platform.CanPrefetch == false)
                return; // not supported

            if (pagesToPrefetch.Count == 0)
                return;

            var entries = new Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY[pagesToPrefetch.Count];
            for (int i = 0; i < entries.Length; i++)
            {
                entries[i].NumberOfBytes = (IntPtr)(4 * PageSize);
                entries[i].VirtualAddress = AcquirePagePointer(null, pagesToPrefetch[i]);
            }

            fixed (Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY* entriesPtr = entries)
            {
                Win32MemoryMapNativeMethods.PrefetchVirtualMemory(Win32Helper.CurrentProcess,
                    (UIntPtr)PagerState.AllocationInfos.Length, entriesPtr, 0);
            }
        }

        public override void TryPrefetchingWholeFile()
        {
            if (Sparrow.Platform.CanPrefetch == false)
                return; // not supported

            var pagerState = PagerState;
            var entries =
                stackalloc Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY[pagerState.AllocationInfos.Length];

            for (var i = 0; i < pagerState.AllocationInfos.Length; i++)
            {
                entries[i].VirtualAddress = pagerState.AllocationInfos[i].BaseAddress;
                entries[i].NumberOfBytes = (IntPtr)pagerState.AllocationInfos[i].Size;
            }

            if (Win32MemoryMapNativeMethods.PrefetchVirtualMemory(Win32Helper.CurrentProcess,
                (UIntPtr)pagerState.AllocationInfos.Length, entries, 0) == false)
                throw new Win32Exception();
        }


        public override void MaybePrefetchMemory(List<TreePage> sortedPages)
        {
            if (Sparrow.Platform.CanPrefetch == false)
                return; // not supported

            if (sortedPages.Count == 0)
                return;

            var ranges = SortedPagesToList(sortedPages);
            fixed (Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY* entries = ranges.ToArray())
            {
                Win32MemoryMapNativeMethods.PrefetchVirtualMemory(Win32Helper.CurrentProcess,
                    (UIntPtr)ranges.Count,
                    entries, 0);
            }
        }
    }
}
