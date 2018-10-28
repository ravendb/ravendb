using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Platform.Posix;
using Sparrow.Utils;
using Voron.Global;
using Voron.Impl;
using Voron.Util.Settings;

namespace Voron.Platform.Posix
{
    public sealed unsafe class PosixMemoryMapPager : PosixAbstractPager
    {
        private readonly StorageEnvironmentOptions _options;
        public readonly long SysPageSize;
        private long _totalAllocationSize;
        private readonly bool _isSyncDirAllowed;
        private readonly bool _copyOnWriteMode;
        public override long TotalAllocationSize => _totalAllocationSize;
        public PosixMemoryMapPager(StorageEnvironmentOptions options, VoronPathSetting file, long? initialFileSize = null,
            bool usePageProtection = false) : base(options, usePageProtection)
        {
            _options = options;
            FileName = file;
            _copyOnWriteMode = options.CopyOnWriteMode && file.FullPath.EndsWith(Constants.DatabaseFilename);
            _isSyncDirAllowed = Syscall.CheckSyncDirectoryAllowed(FileName.FullPath);

            PosixHelper.EnsurePathExists(FileName.FullPath);

            _fd = Syscall.open(file.FullPath, OpenFlags.O_RDWR | PerPlatformValues.OpenFlags.O_CREAT,
                              FilePermissions.S_IWUSR | FilePermissions.S_IRUSR);
            if (_fd == -1)
            {
                var err = Marshal.GetLastWin32Error();
                Syscall.ThrowLastError(err, "when opening " + file);
            }

            SysPageSize = Syscall.sysconf(PerPlatformValues.SysconfNames._SC_PAGESIZE);

            if (SysPageSize <= 0) // i.e. -1 because _SC_PAGESIZE defined differently on various platforms
            {
                var err = Marshal.GetLastWin32Error();
                Syscall.ThrowLastError(err, "Got SysPageSize <= 0 for " + FileName);
            }

            _totalAllocationSize = GetFileSize();

            if (_totalAllocationSize == 0 && initialFileSize.HasValue)
            {
                _totalAllocationSize = NearestSizeToPageSize(initialFileSize.Value);
            }

            if (_totalAllocationSize == 0 || _totalAllocationSize % SysPageSize != 0 ||
                _totalAllocationSize != GetFileSize())
            {
                _totalAllocationSize = NearestSizeToPageSize(_totalAllocationSize);
                PosixHelper.AllocateFileSpace(_options, _fd, _totalAllocationSize, file.FullPath);
            }

            if (_isSyncDirAllowed && Syscall.SyncDirectory(file.FullPath) == -1)
            {
                var err = Marshal.GetLastWin32Error();
                Syscall.ThrowLastError(err, "sync dir for " + file);
            }

            NumberOfAllocatedPages = _totalAllocationSize / Constants.Storage.PageSize;

            SetPagerState(CreatePagerState());
        }


        private long NearestSizeToPageSize(long size)
        {
            if (size == 0)
                return SysPageSize * 16;

            var mod = size % SysPageSize;
            if (mod == 0)
            {
                return size;
            }
            return ((size / SysPageSize) + 1) * SysPageSize;
        }

        private long GetFileSize()
        {
            FileInfo fi = new FileInfo(FileName.FullPath);
            return fi.Length;

        }

        protected override string GetSourceName()
        {
            return "mmap: " + _fd + " " + FileName;
        }

        protected internal override PagerState AllocateMorePages(long newLength)
        {
            if (DisposeOnceRunner.Disposed)
                ThrowAlreadyDisposedException();
            var newLengthAfterAdjustment = NearestSizeToPageSize(newLength);

            if (newLengthAfterAdjustment <= _totalAllocationSize)
                return null;

            var allocationSize = newLengthAfterAdjustment - _totalAllocationSize;

            PosixHelper.AllocateFileSpace(_options, _fd, _totalAllocationSize + allocationSize, FileName.FullPath);

            if (DeleteOnClose == false && _isSyncDirAllowed && Syscall.SyncDirectory(FileName.FullPath) == -1)
            {
                var err = Marshal.GetLastWin32Error();
                Syscall.ThrowLastError(err);
            }

            PagerState newPagerState = CreatePagerState();
            if (newPagerState == null)
            {
                var errorMessage = string.Format(
                    "Unable to allocate more pages - unsuccessfully tried to allocate continuous block of virtual memory with size = {0:##,###;;0} bytes",
                    (_totalAllocationSize + allocationSize));

                throw new OutOfMemoryException(errorMessage);
            }

            newPagerState.DebugVerify(newLengthAfterAdjustment);

            SetPagerState(newPagerState);

            _totalAllocationSize += allocationSize;
            NumberOfAllocatedPages = _totalAllocationSize / Constants.Storage.PageSize;

            return newPagerState;
        }


        private PagerState CreatePagerState()
        {
            var fileSize = GetFileSize();
            var mmflags = _copyOnWriteMode ? MmapFlags.MAP_PRIVATE : MmapFlags.MAP_SHARED;
            var startingBaseAddressPtr = Syscall.mmap64(IntPtr.Zero, (UIntPtr)fileSize,
                                                      MmapProts.PROT_READ | MmapProts.PROT_WRITE,
                                                      mmflags, _fd, 0L);

            if (startingBaseAddressPtr.ToInt64() == -1) //system didn't succeed in mapping the address where we wanted
            {
                var err = Marshal.GetLastWin32Error();

                Syscall.ThrowLastError(err, "mmap on " + FileName);
            }

            NativeMemory.RegisterFileMapping(FileName.FullPath, startingBaseAddressPtr, fileSize, GetAllocatedInBytes);

            var allocationInfo = new PagerState.AllocationInfo
            {
                BaseAddress = (byte*)startingBaseAddressPtr.ToPointer(),
                Size = fileSize,
                MappedFile = null
            };

            var newPager = new PagerState(this)
            {
                Files = null, // unused
                MapBase = allocationInfo.BaseAddress,
                AllocationInfos = new[] { allocationInfo }
            };

            return newPager;
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
                        var result = Syscall.msync(new IntPtr(alloc.BaseAddress), (UIntPtr)alloc.Size, MsyncFlags.MS_SYNC);
                        if (result == -1)
                        {
                            var err = Marshal.GetLastWin32Error();
                            Syscall.ThrowLastError(err, "msync on " + FileName);
                        }
                    }
                    metric.IncrementSize(totalUnsynced);
                }
            }
            finally
            {
                currentState.Release();
            }
        }


        public override string ToString()
        {
            return FileName.FullPath;
        }

        public override void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            base.ReleaseAllocationInfo(baseAddress, size);
            var ptr = new IntPtr(baseAddress);
            var result = Syscall.munmap(ptr, (UIntPtr)size);
            if (result == -1)
            {
                var err = Marshal.GetLastWin32Error();
                Syscall.ThrowLastError(err, "munmap " + FileName);
            }
            NativeMemory.UnregisterFileMapping(FileName.FullPath, ptr, size);
        }

        internal override void ProtectPageRange(byte* start, ulong size, bool force = false)
        {
            if (size == 0)
                return;

            if (UsePageProtection || force)
            {
                if (Syscall.mprotect(new IntPtr(start), size, ProtFlag.PROT_READ) == 0)
                    return;
                var err = Marshal.GetLastWin32Error();
                Debugger.Break();
            }
        }

        internal override void UnprotectPageRange(byte* start, ulong size, bool force = false)
        {
            if (size == 0)
                return;

            if (UsePageProtection || force)
            {
                if (Syscall.mprotect(new IntPtr(start), size, ProtFlag.PROT_READ | ProtFlag.PROT_WRITE) == 0)
                    return;
                var err = Marshal.GetLastWin32Error();
                Debugger.Break();
            }
        }
    }
}
