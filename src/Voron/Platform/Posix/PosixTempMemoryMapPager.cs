using System;
using System.Runtime.InteropServices;
using Sparrow.Utils;
using Voron.Impl;

namespace Voron.Platform.Posix
{
    /// <summary>
    /// In Windows, we use page file based memory mapped file pager.
    /// In Linux, we cannot use the same approach, because using shm_open (similar to anonymous memory mapped file in Windows)
    /// will not actually commit the memory reservation, and it is actually possible that we might allocate memory "successfully"
    /// but only get around to actually using it later. In this case, Linux will realize that it can't meet its promise about using
    /// this memory, and it will throw its hands up in the air and give up. At this point, the OOM killer will get involved and assasinate
    /// us in cold blood, all for believing what the OS said. 
    /// This has to do with overcommit and the process model for Linux requiring duplicate memory at fork().
    /// 
    /// In short, it means that to be reliable, we cannot use swap space in Linux for anything that may be large. We have to create temporary
    /// files for that purpose (so we'll get assured allocation of space on disk, and then be able to mmap them).
    /// 
    /// </summary>
    public unsafe class PosixTempMemoryMapPager : PosixAbstractPager
    {
        private int _fd;
        public readonly long SysPageSize;
        private long _totalAllocationSize;
        
        public PosixTempMemoryMapPager(StorageEnvironmentOptions options, string file, long? initialFileSize = null)
            : base(options)
        {
            FileName = file;
            PosixHelper.EnsurePathExists(file);

            _fd = Syscall.open(FileName, OpenFlags.O_RDWR | OpenFlags.O_CREAT | OpenFlags.O_EXCL,
                FilePermissions.S_IWUSR | FilePermissions.S_IRUSR);
                
            if (_fd == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }
            DeleteOnClose = true;

            SysPageSize = Syscall.sysconf(SysconfName._SC_PAGESIZE);

            _totalAllocationSize = NearestSizeToPageSize(initialFileSize ?? _totalAllocationSize);
            PosixHelper.AllocateFileSpace(_fd, (ulong)_totalAllocationSize, FileName);

            NumberOfAllocatedPages = _totalAllocationSize / PageSize;
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

        protected override string GetSourceName()
        {
            return "shm_open mmap: " + _fd + " " + FileName;
        }

        protected override PagerState AllocateMorePages(long newLength)
        {
            if (Disposed)
                ThrowAlreadyDisposedException();

            var newLengthAfterAdjustment = NearestSizeToPageSize(newLength);

            if (newLengthAfterAdjustment <= _totalAllocationSize) //nothing to do
                return null;

            var allocationSize = newLengthAfterAdjustment - _totalAllocationSize;

            PosixHelper.AllocateFileSpace(_fd, (ulong)(_totalAllocationSize + allocationSize), FileName);

            _totalAllocationSize += allocationSize;

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

            NumberOfAllocatedPages = _totalAllocationSize / PageSize;

            return newPagerState;
        }

        private PagerState CreatePagerState()
        {
            var startingBaseAddressPtr = Syscall.mmap(IntPtr.Zero, (UIntPtr)_totalAllocationSize,
                                                      MmapProts.PROT_READ | MmapProts.PROT_WRITE,
                                                      MmapFlags.MAP_SHARED, _fd, IntPtr.Zero);

            if (startingBaseAddressPtr.ToInt64() == -1) //system didn't succeed in mapping the address where we wanted
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }
            NativeMemory.RegisterFileMapping(FileName, startingBaseAddressPtr, _totalAllocationSize);
            var allocationInfo = new PagerState.AllocationInfo
            {
                BaseAddress = (byte*)startingBaseAddressPtr.ToPointer(),
                Size = _totalAllocationSize,
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

        public override void Sync()
        {
            //nothing to do here
        }


        public override string ToString()
        {
            return FileName;
        }

        public override void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            var ptr = new IntPtr(baseAddress);
            var result = Syscall.munmap(ptr, (UIntPtr)size);
            if (result == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }
            NativeMemory.UnregisterFileMapping(FileName, ptr, size);
        }

        public override void Dispose()
        {
            base.Dispose();
            if (_fd != -1)
            {
                // note that the orders of operations is important here, we first unlink the file
                // we are supposed to be the only one using it, so Linux would be ready to delete it
                // and hopefully when we close it, won't waste any time trying to sync the memory state
                // to disk just to discard it
                if (DeleteOnClose)
                {
                    Syscall.unlink(FileName);
                    // explicitly ignoring the result here, there isn't
                    // much we can do to recover from being unable to delete it
                }
                Syscall.close(_fd);
                _fd = -1;
            }
        }
    }
}

