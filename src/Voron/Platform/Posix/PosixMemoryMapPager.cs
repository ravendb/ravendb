using System;
using System.IO;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Utils;
using Voron.Global;
using Voron.Impl;

namespace Voron.Platform.Posix
{
    public unsafe class PosixMemoryMapPager : PosixAbstractPager
    {
        private int _fd;
        public readonly long SysPageSize;
        private long _totalAllocationSize;
        private readonly bool _isSyncDirAllowed;
        private bool _copyOnWriteMode;
        
        public PosixMemoryMapPager(StorageEnvironmentOptions options,string file, long? initialFileSize = null):base(options)
        {
            FileName = file;
            _copyOnWriteMode = options.CopyOnWriteMode && file.EndsWith(Constants.DatabaseFilename);
            _isSyncDirAllowed = PosixHelper.CheckSyncDirectoryAllowed(FileName);

            PosixHelper.EnsurePathExists(FileName);

            _fd = Syscall.open(file, OpenFlags.O_RDWR | OpenFlags.O_CREAT,
                              FilePermissions.S_IWUSR | FilePermissions.S_IRUSR);
            if (_fd == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }

            SysPageSize = Syscall.sysconf(SysconfName._SC_PAGESIZE);

            _totalAllocationSize = GetFileSize();
            
            if (_totalAllocationSize == 0 && initialFileSize.HasValue)
            {
                _totalAllocationSize = NearestSizeToPageSize(initialFileSize.Value);
            }
            if (_totalAllocationSize == 0 || _totalAllocationSize % SysPageSize != 0 ||
                _totalAllocationSize != GetFileSize())
            {
                _totalAllocationSize = NearestSizeToPageSize(_totalAllocationSize);
                PosixHelper.AllocateFileSpace(_fd, (ulong) _totalAllocationSize);
            }

            if (_isSyncDirAllowed && PosixHelper.SyncDirectory(file) == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }

            NumberOfAllocatedPages = _totalAllocationSize / _pageSize;
            PagerState.Release();
            PagerState = CreatePagerState();
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
            FileInfo fi = new FileInfo(FileName);
            return fi.Length;
            
        }

        protected override string GetSourceName()
        {
            return "mmap: " + _fd + " " + FileName;
        }

        protected override PagerState AllocateMorePages(long newLength)
        {
            if (Disposed)
                ThrowAlreadyDisposedException();
            var newLengthAfterAdjustment = NearestSizeToPageSize(newLength);

            if (newLengthAfterAdjustment <= _totalAllocationSize)
                return null;

            var allocationSize = newLengthAfterAdjustment - _totalAllocationSize;

            PosixHelper.AllocateFileSpace(_fd, (ulong) (_totalAllocationSize + allocationSize));

            if (_isSyncDirAllowed && PosixHelper.SyncDirectory(FileName) == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
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

            var tmp = PagerState;
            PagerState = newPagerState;
            tmp.Release(); //replacing the pager state --> so one less reference for it

            _totalAllocationSize += allocationSize;
            NumberOfAllocatedPages = _totalAllocationSize/ _pageSize;

            return newPagerState;
        }


        private PagerState CreatePagerState()
        {
            var fileSize = GetFileSize();
            var mmflags = _copyOnWriteMode ? MmapFlags.MAP_PRIVATE : MmapFlags.MAP_SHARED;
            var startingBaseAddressPtr = Syscall.mmap(IntPtr.Zero, (ulong)fileSize,
                                                      MmapProts.PROT_READ | MmapProts.PROT_WRITE,
                                                      mmflags, _fd, 0);

            if (startingBaseAddressPtr.ToInt64() == -1) //system didn't succeed in mapping the address where we wanted
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }

            NativeMemory.RegisterFileMapping(FileName, fileSize);

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

            newPager.AddRef(); // one for the pager
            return newPager;
        }

        public override void Sync()
        {
            long totalSize = 0;
            foreach (var allocationInfo in PagerState.AllocationInfos)
            {
                totalSize += allocationInfo.Size;
            }

            //TODO: Is it worth it to change to just one call for msync for the entire file?
            using (Options.IoMetrics.MeterIoRate(FileName,IoMetrics.MeterType.Sync, totalSize))
            {
                foreach (var alloc in PagerState.AllocationInfos)
                {
                    var result = Syscall.msync(new IntPtr(alloc.BaseAddress), (ulong)alloc.Size, MsyncFlags.MS_SYNC);
                    if (result == -1)
                    {
                        var err = Marshal.GetLastWin32Error();
                        PosixHelper.ThrowLastError(err);
                    }
                }
            }
        }


        public override string ToString()
        {
            return FileName;
        }

        public override void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            var result = Syscall.munmap(new IntPtr(baseAddress), (ulong)size);
            if (result == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }
            NativeMemory.UnregisterFileMapping(FileName, size);
        }

        public override void Dispose()
        {
            base.Dispose();
            if (_fd != -1)
            {
                Syscall.close(_fd);
                _fd = -1;
            }
        }
    }
}
