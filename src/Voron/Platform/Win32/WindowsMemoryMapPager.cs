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
using Sparrow.Binary;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Win32;
using Sparrow.Utils;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Util;
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

        private const byte EvenPrefetchCountMask = 0x70;
        private const byte EvenPrefetchMaskShift = 4;
        private const byte OddPrefetchCountMask = 0x07;
        private const byte AlreadyPrefetch = 7;

        private readonly int _prefetchSegmentSize;       
        private readonly int _prefetchResetThreshold; 

        private readonly int _segmentShift;
        private int _refreshCounter = 0;
        private byte[] _prefetchTable;


        public WindowsMemoryMapPager(StorageEnvironmentOptions options, VoronPathSetting file,
            long? initialFileSize = null,
            Win32NativeFileAttributes fileAttributes = Win32NativeFileAttributes.Normal,
            Win32NativeFileAccess access = Win32NativeFileAccess.GenericRead | Win32NativeFileAccess.GenericWrite,
            bool usePageProtection = false)
            : base(options, usePageProtection)
        {                        
            this._segmentShift = Bits.MostSignificantBit(options.PrefetchSegmentSize);            

            this._prefetchSegmentSize = 1 << this._segmentShift;
            this._prefetchResetThreshold = (int)((float)options.PrefetchResetThreshold / this._prefetchSegmentSize);

            Debug.Assert((_prefetchSegmentSize - 1) >> this._segmentShift == 0);
            Debug.Assert(_prefetchSegmentSize >> this._segmentShift == 1);

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
                throw new IOException("Failed to open file storage of Win32MemoryMapPager for " + file,
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
            long numberOfAllocatedSegments = (_totalAllocationSize / _prefetchSegmentSize) + 1;
            this._prefetchTable = new byte[(numberOfAllocatedSegments / 2) + 1];

            SetPagerState(CreatePagerState());
        }

        public override byte* AcquirePagePointer(IPagerLevelTransactionState tx, long pageNumber, PagerState pagerState = null)
        {
            // We need to decide what pager we are going to use right now or risk inconsistencies when performing prefetches from disk.
            var state = pagerState ?? _pagerState;

            if (PlatformDetails.CanPrefetch)
            {
                long segmentNumber = (pageNumber * Constants.Storage.PageSize) >> this._segmentShift;                

                int segmentState = GetSegmentState(segmentNumber);
                if (segmentState < AlreadyPrefetch)
                {
                    // We update the current segment counter
                    segmentState++;

                    int previousSegmentState = GetSegmentState(segmentNumber - 1);
                    if (previousSegmentState == AlreadyPrefetch)
                    {
                        segmentState = AlreadyPrefetch;
                    }

                    SetSegmentState(segmentNumber, segmentState);

                    if (segmentState == AlreadyPrefetch)
                    {
                        MaybePrefetchSegment(state.MapBase, segmentNumber);
                        _refreshCounter++;

                        if (_refreshCounter > _prefetchResetThreshold)
                            ResetPrefetchTable();
                    }                        
                }
            }
           
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
            PagerState newPagerState = null;

#if VALIDATE
            // If we're on validate more, we don't want to allocate continuous pages because this
            // introduces weird conditions on the protection and unprotection routines (we have to
            // track boundaries, which is more complex than we're willing to do)
            newPagerState = CreatePagerState();

            SetPagerState(newPagerState);

            PagerState.DebugVerify(newLengthAfterAdjustment);
#else
            if (TryAllocateMoreContinuousPages(allocationSize) == false)
            {
                newPagerState = CreatePagerState();

                SetPagerState(newPagerState);

                PagerState.DebugVerify(newLengthAfterAdjustment);
            }
#endif

            _totalAllocationSize += allocationSize;
            NumberOfAllocatedPages = _totalAllocationSize / Constants.Storage.PageSize;

            long numberOfAllocatedSegments = (_totalAllocationSize / _prefetchSegmentSize) + 1;

            var oldPrefetchTable = this._prefetchTable;
            this._prefetchTable = new byte[(numberOfAllocatedSegments / 2) + 1];
            Array.Copy(oldPrefetchTable, this._prefetchTable, oldPrefetchTable.Length);

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

            if (PlatformDetails.CanPrefetch)
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

            ProtectPageRange(newMappingBaseAddress, (ulong)allocationSize);

            NativeMemory.RegisterFileMapping(_fileInfo.FullName, new IntPtr(newMappingBaseAddress), allocationSize, GetAllocatedInBytes);

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

                var errorMessage = string.Format(
                    "Unable to allocate more pages - unsuccessfully tried to allocate continuous block of virtual memory with size = {0:##,###;;0} bytes",
                    (_fileStream.Length));

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

            var newPager = new PagerState(this)
            {
                Files = new[] { mmf },
                MapBase = startingBaseAddressPtr,
                AllocationInfos = new[] { allocationInfo }
            };

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


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetSegmentState(long segment)
        {
            if (segment < 0)
                return AlreadyPrefetch;

            byte value = this._prefetchTable[segment / 2];
            if (segment % 2 == 0)
            {
                // The actual value is in the high byte.
                value = (byte)(value >> EvenPrefetchMaskShift);
            }
            else
            {
                value = (byte)(value & OddPrefetchCountMask);
            }

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetSegmentState(long segment, int state)
        {
            byte value = this._prefetchTable[segment / 2];
            if (segment % 2 == 0)
            {
                // The actual value is in the high byte.
                value = (byte)((value & OddPrefetchCountMask) | (state << EvenPrefetchMaskShift));
            }
            else
            {
                value = (byte)((value & EvenPrefetchCountMask) | state);
            }

            this._prefetchTable[segment / 2] = value;
        }

        private void ResetPrefetchTable()
        {
            this._refreshCounter = 0;
            
            // We will zero out the whole table to reset the prefetching behavior. 
            Array.Clear(this._prefetchTable, 0, this._prefetchTable.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void MaybePrefetchSegment(byte* baseAddress, long segment)
        {
            Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY entry;
            entry.NumberOfBytes = (IntPtr)_prefetchSegmentSize;
            entry.VirtualAddress = baseAddress + segment * _prefetchSegmentSize;

            Win32MemoryMapNativeMethods.PrefetchVirtualMemory(Win32Helper.CurrentProcess, (UIntPtr)1, &entry, 0);
        }

        public override void MaybePrefetchMemory(Span<long> pagesToPrefetch)
        {
            if (PlatformDetails.CanPrefetch == false)
                return; // not supported

            if (pagesToPrefetch.Length == 0)
                return;

            // PERF: We dont acquire pointer here to avoid all the overhead of doing so; instead we calculate the proper place based on 
            //       base address from the pager.
            byte* baseAddress = this._pagerState.MapBase;

            const int StackSpace = 16;

            int prefetchIdx = 0;
            Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY* toPrefetch = stackalloc Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY[StackSpace];
            
            foreach ( long pageNumber in pagesToPrefetch)
            {
                long segmentNumber = (pageNumber * Constants.Storage.PageSize) >> this._segmentShift;

                int segmentState = GetSegmentState(segmentNumber);
                if (segmentState < AlreadyPrefetch)
                {
                    // We update the current segment counter
                    segmentState++;

                    int previousSegmentState = GetSegmentState(segmentNumber - 1);
                    if (previousSegmentState == AlreadyPrefetch)
                    {
                        segmentState = AlreadyPrefetch;
                    }

                    SetSegmentState(segmentNumber, segmentState);

                    if (segmentState == AlreadyPrefetch)
                    {
                        // Prepare the segment information. 
                        toPrefetch[prefetchIdx].NumberOfBytes = (IntPtr)_prefetchSegmentSize;
                        toPrefetch[prefetchIdx].VirtualAddress = baseAddress + segmentNumber * _prefetchSegmentSize;
                        prefetchIdx++;
                        _refreshCounter++;

                        if (prefetchIdx >= StackSpace)
                        {
                            // We dont have enough space, so we send the batch to the kernel
                            Win32MemoryMapNativeMethods.PrefetchVirtualMemory(Win32Helper.CurrentProcess, (UIntPtr)StackSpace, toPrefetch, 0);
                            prefetchIdx = 0;
                        }
                    }
                }
            }

            if (prefetchIdx != 0)
            {
                // We dont have enough space, so we send the batch to the kernel
                Win32MemoryMapNativeMethods.PrefetchVirtualMemory(Win32Helper.CurrentProcess, (UIntPtr)prefetchIdx, toPrefetch, 0);
            }

            if (_refreshCounter > _prefetchResetThreshold)
                ResetPrefetchTable();
        }

        public override int CopyPage(I4KbBatchWrites destwI4KbBatchWrites, long p, PagerState pagerState)
        {
            return CopyPageImpl(destwI4KbBatchWrites, p, pagerState);
        }

        public override void TryPrefetchingWholeFile()
        {
            if (PlatformDetails.CanPrefetch == false)
                return; // not supported

            var pagerState = PagerState;
            var entries = stackalloc Win32MemoryMapNativeMethods.WIN32_MEMORY_RANGE_ENTRY[pagerState.AllocationInfos.Length];

            for (var i = 0; i < pagerState.AllocationInfos.Length; i++)
            {
                entries[i].VirtualAddress = pagerState.AllocationInfos[i].BaseAddress;
                entries[i].NumberOfBytes = (IntPtr)pagerState.AllocationInfos[i].Size;
            }

            if (Win32MemoryMapNativeMethods.PrefetchVirtualMemory(Win32Helper.CurrentProcess,
                (UIntPtr)pagerState.AllocationInfos.Length, entries, 0) == false)
                throw new Win32Exception(Marshal.GetLastWin32Error(), "Failed to Prefetch Vitrual Memory of file " + FileName);
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
