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
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Trees;
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

        private static readonly bool CanPrefetch = IsWindows8OrNewer();

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

        public Win32MemoryMapPager(string file,
            long? initialFileSize = null,
                                   Win32NativeFileAttributes options = Win32NativeFileAttributes.Normal,
                                   Win32NativeFileAccess access = Win32NativeFileAccess.GenericRead | Win32NativeFileAccess.GenericWrite)
        {
            Win32NativeMethods.SYSTEM_INFO systemInfo;
            Win32NativeMethods.GetSystemInfo(out systemInfo);

            AllocationGranularity = systemInfo.allocationGranularity;

            _access = access;
            _memoryMappedFileAccess = _access == Win32NativeFileAccess.GenericRead
                ? MemoryMappedFileAccess.Read
                : MemoryMappedFileAccess.ReadWrite;

            _handle = Win32NativeFileMethods.CreateFile(file, access,
                                                        Win32NativeFileShare.Read | Win32NativeFileShare.Write | Win32NativeFileShare.Delete, IntPtr.Zero,
                                                        Win32NativeFileCreationDisposition.OpenAlways, options, IntPtr.Zero);
            if (_handle.IsInvalid)
            {
                int lastWin32ErrorCode = Marshal.GetLastWin32Error();
                throw new IOException("Failed to open file storage of Win32MemoryMapPage+r",
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

            NumberOfAllocatedPages = _totalAllocationSize / PageSize;
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

        public override void AllocateMorePages(Transaction tx, long newLength)
        {
            ThrowObjectDisposedIfNeeded();

            var newLengthAfterAdjustment = NearestSizeToAllocationGranularity(newLength);

            if (newLengthAfterAdjustment <= _totalAllocationSize)
                return;

            var allocationSize = newLengthAfterAdjustment - _totalAllocationSize;

            Win32NativeFileMethods.SetFileLength(_handle, _totalAllocationSize + allocationSize);
            if (TryAllocateMoreContinuousPages(allocationSize) == false)
            {
                RefreshMappedView(tx);
                PagerState.DebugVerify(newLengthAfterAdjustment);
            }

            _totalAllocationSize += allocationSize;
            NumberOfAllocatedPages = _totalAllocationSize / PageSize;
        }

        public void RefreshMappedView(Transaction tx)
        {
            PagerState newPagerState = CreatePagerState();

            if (tx != null)
            {
                tx.EnsurePagerStateReference(newPagerState);
            }

            var tmp = PagerState;
            PagerState = newPagerState;
            tmp.Release(); //replacing the pager state --> so one less reference for it
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

            if (CanPrefetch == false)
                return true; // not supported

            // We are asking to allocate pages. It is a good idea that they should be already in memory to only cause a single page fault (as they are continuous).
            Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY entry;
            entry.VirtualAddress = allocationInfo.BaseAddress;
            entry.NumberOfBytes = (IntPtr)allocationInfo.Size;

            Win32MemoryMapNativeMethods.PrefetchVirtualMemory(Win32NativeMethods.GetCurrentProcess(), (UIntPtr)1, &entry, 0);

            return true;
        }

        private PagerState.AllocationInfo RemapViewOfFileAtAddress(long allocationSize, ulong offsetInFile, byte* baseAddress)
        {
            var offset = new SplitValue { Value = offsetInFile };

            var mmf = MemoryMappedFile.CreateFromFile(_fileStream, null, _fileStream.Length,
                _memoryMappedFileAccess,
                null, HandleInheritability.None, true);

            var newMappingBaseAddress = Win32MemoryMapNativeMethods.MapViewOfFileEx(mmf.SafeMemoryMappedFileHandle.DangerousGetHandle(),
                Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read | Win32MemoryMapNativeMethods.NativeFileMapAccessType.Write,
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
                null, HandleInheritability.None, true);

            var fileMappingHandle = mmf.SafeMemoryMappedFileHandle.DangerousGetHandle();
            var mmFileAccessType = _access == Win32NativeFileAccess.GenericRead
                ? Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read
                    : Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read | Win32MemoryMapNativeMethods.NativeFileMapAccessType.Write;

            var startingBaseAddressPtr = Win32MemoryMapNativeMethods.MapViewOfFileEx(fileMappingHandle,
                mmFileAccessType,
                0, 0,
                UIntPtr.Zero, //map all what was "reserved" in CreateFileMapping on previous row
                null);


            if (startingBaseAddressPtr == (byte*)0) //system didn't succeed in mapping the address where we wanted
            {
                var errorMessage = string.Format(
                    "Unable to allocate more pages - unsuccessfully tried to allocate continuous block of virtual memory with size = {0:##,###;;0} bytes",
                    (_fileStream.Length));

                throw new OutOfMemoryException(errorMessage, new Win32Exception());
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
            return "MemMap: " + _fileInfo.Name;
        }
        
        public override void Sync()
        {
            ThrowObjectDisposedIfNeeded();

            foreach (var allocationInfo in PagerState.AllocationInfos)
            {
                if (Win32MemoryMapNativeMethods.FlushViewOfFile(allocationInfo.BaseAddress, new IntPtr(allocationInfo.Size)) == false)
                    throw new Win32Exception();
            }

            if (Win32MemoryMapNativeMethods.FlushFileBuffers(_handle) == false)
                throw new Win32Exception();
        }


        public override string ToString()
        {
            return _fileInfo.Name;
        }


        public override void Dispose()
        {
            if (Disposed)
                return;

            if (_fileStream != null)
                _fileStream.Dispose();
            if (_handle != null)
                _handle.Close();
            if (DeleteOnClose && _fileInfo != null)
                _fileInfo.Delete();

            base.Dispose();
        }

        public override void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            if (Win32MemoryMapNativeMethods.UnmapViewOfFile(baseAddress) == false)
                throw new Win32Exception();
        }

        private static bool IsWindows8OrNewer()
        {
            var os = Environment.OSVersion;
            return os.Platform == PlatformID.Win32NT &&
                   (os.Version.Major > 6 || (os.Version.Major == 6 && os.Version.Minor >= 2));
        }

        public override void MaybePrefetchMemory(List<Page> sortedPages)
        {
            if (sortedPages.Count == 0)
                return;

            if (CanPrefetch == false)
                return; // not supported

            var list = new List<Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY>();

            long lastPage = -1;
            const int numberOfPagesInBatch = 8;
            int sizeInPages = numberOfPagesInBatch; // OS uses 32K when you touch a page, let us reuse this
            foreach (var page in sortedPages)
            {
                if (lastPage == -1)
                {
                    lastPage = page.PageNumber;
                }

                var numberOfPagesInLastPage = page.IsOverflow == false
                    ? 1
                    : GetNumberOfOverflowPages(page.OverflowSize);

                var endPage = page.PageNumber + numberOfPagesInLastPage - 1;

                if (endPage <= lastPage + sizeInPages)
                    continue; // already within the allocation granularity we have

                if (page.PageNumber <= lastPage + sizeInPages + numberOfPagesInBatch)
                {
                    while (endPage > lastPage + sizeInPages)
                    {
                        sizeInPages += numberOfPagesInBatch;
                    }

                    continue;
                }

                list.Add(new Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY
                {
                    NumberOfBytes = (IntPtr)(sizeInPages * AbstractPager.PageSize),
                    VirtualAddress = AcquirePagePointer(null, lastPage)
                });
                lastPage = page.PageNumber;
                sizeInPages = numberOfPagesInBatch;
                while (endPage > lastPage + sizeInPages)
                {
                    sizeInPages += numberOfPagesInBatch;
                }
            }
            list.Add(new Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY
            {
                NumberOfBytes = (IntPtr)(sizeInPages * PageSize),
                VirtualAddress = AcquirePagePointer(null, lastPage)
            });

            fixed (Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY* entries = list.ToArray())
            {
                Win32MemoryMapNativeMethods.PrefetchVirtualMemory(Win32NativeMethods.GetCurrentProcess(),
                    (UIntPtr)list.Count,
                    entries, 0);
            }
        }

        public override void MaybePrefetchMemory(List<long> pagesToPrefetch)
        {
            if (CanPrefetch == false)
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
                Win32MemoryMapNativeMethods.PrefetchVirtualMemory(Win32NativeMethods.GetCurrentProcess(), (UIntPtr)PagerState.AllocationInfos.Length, entriesPtr, 0);
            }
        }

        public override void TryPrefetchingWholeFile()
        {
            if (CanPrefetch == false)
                return; // not supported

            var entries = stackalloc Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY[PagerState.AllocationInfos.Length];

            for (int i = 0; i < PagerState.AllocationInfos.Length; i++)
            {
                entries[i].VirtualAddress = PagerState.AllocationInfos[i].BaseAddress;
                entries[i].NumberOfBytes = (IntPtr)PagerState.AllocationInfos[i].Size;
            }

            // We're deliberately ignoring the return value here and not throwing an exception if it returns false.
            // 
            // This call is merely an optimization that can fail in low-memory conditions.
            // See https://msdn.microsoft.com/en-us/library/windows/desktop/hh780543(v=vs.85).aspx
            // 
            // Because of that, and because this call regularly fails on Windows Azure Web Apps (http://issues.hibernatingrhinos.com/issue/RavenDB-4670),
            // we ignore the return value and don't throw if it fails.
            Win32MemoryMapNativeMethods.PrefetchVirtualMemory(
                Win32NativeMethods.GetCurrentProcess(),
                (UIntPtr)PagerState.AllocationInfos.Length,
                entries,
                0);
        }
    }
}
