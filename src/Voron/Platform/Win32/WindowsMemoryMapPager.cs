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
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Win32;
using Sparrow.Utils;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Util.Settings;
using static Voron.Platform.Win32.Win32NativeMethods;

namespace Voron.Platform.Win32
{
    public unsafe class WindowsMemoryMapPager : AbstractPager
    {
        public const int AllocationGranularity = 64 * Constants.Size.Kilobyte;
        private long _totalAllocationSize;
        private readonly FileInfo _fileInfo;
        private readonly FileStream _fileStream;
        private readonly SafeFileHandle _handle;
        private readonly Win32NativeFileAttributes _fileAttributes;
        private readonly Win32NativeFileAccess _access;
        private readonly MemoryMappedFileAccess _memoryMappedFileAccess;
        private readonly bool _copyOnWriteMode;
        private readonly Logger _logger;
        public override long TotalAllocationSize => _totalAllocationSize;

        [StructLayout(LayoutKind.Explicit)]
        public struct SplitValue
        {
            [FieldOffset(0)]
            public ulong Value;

            [FieldOffset(0)]
            public uint Low;

            [FieldOffset(4)]
            public uint High;
        }

        public WindowsMemoryMapPager(StorageEnvironmentOptions options, VoronPathSetting file,
            long? initialFileSize = null,
            Win32NativeFileAttributes fileAttributes = Win32NativeFileAttributes.Normal,
            Win32NativeFileAccess access = Win32NativeFileAccess.GenericRead | Win32NativeFileAccess.GenericWrite,
            bool usePageProtection = false)
            : base(options, !fileAttributes.HasFlag(Win32NativeFileAttributes.Temporary), usePageProtection)
        {
            SYSTEM_INFO systemInfo;
            GetSystemInfo(out systemInfo);
            FileName = file;
            _logger = LoggingSource.Instance.GetLogger<StorageEnvironment>($"Pager-{file}");

            _access = access;
            _copyOnWriteMode = Options.CopyOnWriteMode && FileName.FullPath.EndsWith(Constants.DatabaseFilename);
            if (_copyOnWriteMode)
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
            _fileAttributes = fileAttributes;

            _handle = Win32NativeFileMethods.CreateFile(file.FullPath, access,
                                                        Win32NativeFileShare.Read | Win32NativeFileShare.Write | Win32NativeFileShare.Delete, IntPtr.Zero,
                                                        Win32NativeFileCreationDisposition.OpenAlways, fileAttributes, IntPtr.Zero);
            if (_handle.IsInvalid)
            {
                int lastWin32ErrorCode = Marshal.GetLastWin32Error();
                throw new IOException("Failed to open file storage of WinMemoryMapPager for " + file,
                    new Win32Exception(lastWin32ErrorCode));
            }

            _fileInfo = new FileInfo(file.FullPath);
            var drive = _fileInfo.Directory.Root.Name.TrimEnd('\\');

            try
            {
                if (PhysicalDrivePerMountCache.TryGetValue(drive, out UniquePhysicalDriveId) == false)
                    UniquePhysicalDriveId = GetPhysicalDriveId(drive);

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Physical drive '{drive}' unique id = '{UniquePhysicalDriveId}' for file '{file}'");
            }
            catch (Exception ex)
            {
                UniquePhysicalDriveId = 0;
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Failed to determine physical drive Id for drive letter '{drive}', file='{file}'", ex);
            }

            var streamAccessType = _access == Win32NativeFileAccess.GenericRead
                ? FileAccess.Read
                : FileAccess.ReadWrite;
            _fileStream = SafeFileStream.Create(_handle, streamAccessType);

            _totalAllocationSize = _fileInfo.Length;

            if ((access & Win32NativeFileAccess.GenericWrite) == Win32NativeFileAccess.GenericWrite ||
                (access & Win32NativeFileAccess.GenericAll) == Win32NativeFileAccess.GenericAll ||
                (access & Win32NativeFileAccess.FILE_GENERIC_WRITE) == Win32NativeFileAccess.FILE_GENERIC_WRITE)
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

            NumberOfAllocatedPages = _totalAllocationSize / Constants.Storage.PageSize;

            var pager = CreatePagerState();
            if (fileAttributes.HasFlag(Win32NativeFileAttributes.Temporary))
                pager.DiscardDataOnDisk();

            SetPagerState(pager);
        }

        public override byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            // We need to decide what pager we are going to use right now or risk inconsistencies when performing prefetches from disk.
            var state = pagerState ?? _pagerState;

            if (Pal.SysInfo.CanPrefetch && this._pagerState.ShouldPrefetchSegment(pageNumber, out void* virtualAddress, out long bytes))
                Pal.rvn_prefetch_virtual_memory(virtualAddress, bytes, out _);

            return base.AcquirePagePointer(tx, pageNumber, state);
        }

        public static uint GetPhysicalDriveId(string drive)
        {
            var sdn = new StorageDeviceNumber();

            var driveHandle = CreateFile(@"\\.\" + drive, 0, 0, IntPtr.Zero, (uint)CreationDisposition.OPEN_EXISTING, 0, IntPtr.Zero);

            if (driveHandle.ToInt64() == -1)
            {
                int lastWin32ErrorCode = Marshal.GetLastWin32Error();
                throw new IOException("Failed to CreateFile for Drive : " + drive,
                    new Win32Exception(lastWin32ErrorCode));
            }
            try
            {
                int requiredSize;
                if (DeviceIoControl(driveHandle,
                        (int)IoControlCode.IOCTL_STORAGE_GET_DEVICE_NUMBER, IntPtr.Zero, 0, new IntPtr(&sdn), sizeof(StorageDeviceNumber),
                        out requiredSize, IntPtr.Zero) == false)
                {
                    int lastWin32ErrorCode = Marshal.GetLastWin32Error();
                    throw new IOException("Failed to DeviceIoControl for Drive : " + drive,
                        new Win32Exception(lastWin32ErrorCode));
                }
            }
            finally
            {
                CloseHandle(driveHandle);
            }
            return (uint)((int)sdn.DeviceType << 8) + sdn.DeviceNumber;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long NearestSizeToAllocationGranularity(long size)
        {
            var modulos = size % AllocationGranularity;
            if (modulos == 0)
                return Math.Max(size, AllocationGranularity);

            return ((size / AllocationGranularity) + 1) * AllocationGranularity;
        }

        protected internal override PagerState AllocateMorePages(long newLength)
        {
            if (DisposeOnceRunner.Disposed)
                ThrowAlreadyDisposedException();

            var newLengthAfterAdjustment = NearestSizeToAllocationGranularity(newLength);

            if (newLengthAfterAdjustment <= _totalAllocationSize)
                return null;

            var allocationSize = newLengthAfterAdjustment - _totalAllocationSize;

            Win32NativeFileMethods.SetFileLength(_handle, _totalAllocationSize + allocationSize);

            PagerState newPagerState = CreatePagerState();
            newPagerState.CopyPrefetchState(this._pagerState);

            SetPagerState(newPagerState);

            PagerState.DebugVerify(newLengthAfterAdjustment);

            _totalAllocationSize += allocationSize;
            NumberOfAllocatedPages = _totalAllocationSize / Constants.Storage.PageSize;

            return newPagerState;
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
                mmFileAccessType = Win32MemoryMapNativeMethods.NativeFileMapAccessType.Copy;
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
                var innerException = new Win32Exception(Marshal.GetLastWin32Error(), "Failed to MapView of file " + FileName);

                var errorMessage = $"Unable to allocate more pages - unsuccessfully tried to allocate continuous block of virtual memory with size = {(_fileStream.Length):##,###;;0} bytes";

                throw new OutOfMemoryException(errorMessage, innerException);
            }

            NativeMemory.RegisterFileMapping(_fileInfo.FullName, new IntPtr(startingBaseAddressPtr), _fileStream.Length, GetAllocatedInBytes);

            // If we are working on memory validation mode, then protect the pages by default.
            ProtectPageRange(startingBaseAddressPtr, (ulong)_fileStream.Length);

            var allocationInfo = new PagerState.AllocationInfo
            {
                BaseAddress = startingBaseAddressPtr,
                Size = _fileStream.Length,
                MappedFile = mmf
            };

            var newPager = new PagerState(this, Options.PrefetchSegmentSize, Options.PrefetchResetThreshold, allocationInfo);
            return newPager;
        }

        protected override string GetSourceName()
        {
            if (_fileInfo == null)
                return "Unknown";
            return "MemMap: " + _fileInfo.FullName;
        }

        public override void Sync(long totalUnsynced)
        {
            if (DisposeOnceRunner.Disposed)
                ThrowAlreadyDisposedException();

            if ((_fileAttributes & Win32NativeFileAttributes.Temporary) == Win32NativeFileAttributes.Temporary ||
                (_fileAttributes & Win32NativeFileAttributes.DeleteOnClose) == Win32NativeFileAttributes.DeleteOnClose)
                return; // no need to do this


            var currentState = GetPagerStateAndAddRefAtomically();
            try
            {
                using (var metric = Options.IoMetrics.MeterIoRate(FileName.FullPath, IoMetrics.MeterType.DataSync, 0))
                {
                    foreach (var allocationInfo in currentState.AllocationInfos)
                    {
                        metric.IncrementFileSize(allocationInfo.Size);

                        if (
                            Win32MemoryMapNativeMethods.FlushViewOfFile(allocationInfo.BaseAddress,
                                new IntPtr(allocationInfo.Size)) == false)
                        {
                            var lasterr = Marshal.GetLastWin32Error();
                            throw new Win32Exception(lasterr);
                        }
                    }

                    metric.IncrementSize(totalUnsynced);

                    if (Win32MemoryMapNativeMethods.FlushFileBuffers(_handle) == false)
                    {
                        var lasterr = Marshal.GetLastWin32Error();
                        throw new Win32Exception(lasterr);
                    }
                }
            }
            finally
            {
                currentState.Release();
            }
        }


        public override string ToString()
        {
            return _fileInfo.Name;
        }

        protected override void DisposeInternal()
        {
            _fileStream?.Dispose();
            _handle?.Dispose();
            if (DeleteOnClose)
                _fileInfo?.Delete();
        }

        public override void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            base.ReleaseAllocationInfo(baseAddress, size);
            if (Win32MemoryMapNativeMethods.UnmapViewOfFile(baseAddress) == false)
                throw new Win32Exception(Marshal.GetLastWin32Error(), "Failed to UnMapView of file " + FileName);
            NativeMemory.UnregisterFileMapping(_fileInfo.FullName, new IntPtr(baseAddress), size);
        }

        public override int CopyPage(I4KbBatchWrites destwI4KbBatchWrites, long p, PagerState pagerState)
        {
            return CopyPageImpl(destwI4KbBatchWrites, p, pagerState);
        }

        protected internal override void PrefetchRanges(PalDefinitions.PrefetchRanges* list, int count)
        {
            Pal.rvn_prefetch_ranges(list, count, out _);
            // we explicitly ignore the return code here, this is optimization only
        }

        internal override void ProtectPageRange(byte* start, ulong size, bool force = false)
        {
            if (size == 0)
                return;

            if (UsePageProtection || force)
            {
                Win32MemoryProtectMethods.MEMORY_BASIC_INFORMATION memoryInfo1 = new Win32MemoryProtectMethods.MEMORY_BASIC_INFORMATION();
                int vQueryFirstOutput = Win32MemoryProtectMethods.VirtualQuery(start, &memoryInfo1, new UIntPtr(size));
                int vQueryFirstError = Marshal.GetLastWin32Error();

                Win32MemoryProtectMethods.MemoryProtection oldProtection;
                bool status = Win32MemoryProtectMethods.VirtualProtect(start, new UIntPtr(size), Win32MemoryProtectMethods.MemoryProtection.READONLY, out oldProtection);
                if (!status)
                {
                    int vProtectError = Marshal.GetLastWin32Error();

                    Win32MemoryProtectMethods.MEMORY_BASIC_INFORMATION memoryInfo2 = new Win32MemoryProtectMethods.MEMORY_BASIC_INFORMATION();
                    int vQuerySecondOutput = Win32MemoryProtectMethods.VirtualQuery(start, &memoryInfo2, new UIntPtr(size));
                    int vQuerySecondError = Marshal.GetLastWin32Error();
                    Debugger.Break();
                }
            }
        }

        internal override void UnprotectPageRange(byte* start, ulong size, bool force = false)
        {
            if (size == 0)
                return;

            if (UsePageProtection || force)
            {
                Win32MemoryProtectMethods.MEMORY_BASIC_INFORMATION memoryInfo1 = new Win32MemoryProtectMethods.MEMORY_BASIC_INFORMATION();
                int vQueryFirstOutput = Win32MemoryProtectMethods.VirtualQuery(start, &memoryInfo1, new UIntPtr(size));
                int vQueryFirstError = Marshal.GetLastWin32Error();

                Win32MemoryProtectMethods.MemoryProtection oldProtection;
                bool status = Win32MemoryProtectMethods.VirtualProtect(start, new UIntPtr(size), Win32MemoryProtectMethods.MemoryProtection.READWRITE, out oldProtection);
                if (!status)
                {
                    int vProtectError = Marshal.GetLastWin32Error();

                    Win32MemoryProtectMethods.MEMORY_BASIC_INFORMATION memoryInfo2 = new Win32MemoryProtectMethods.MEMORY_BASIC_INFORMATION();
                    int vQuerySecondOutput = Win32MemoryProtectMethods.VirtualQuery(start, &memoryInfo2, new UIntPtr(size));
                    int vQuerySecondError = Marshal.GetLastWin32Error();
                    Debugger.Break();
                }
            }
        }
    }
}
