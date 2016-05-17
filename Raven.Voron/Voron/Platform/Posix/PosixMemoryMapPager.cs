using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Raven.Unix.Native;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Trees;
using Voron.Util;
using Sparrow;

namespace Voron.Platform.Posix
{
    public unsafe class PosixMemoryMapPager : AbstractPager
    {
        private readonly string _file;
        private int _fd;
        public readonly long SysPageSize;
        private long _totalAllocationSize;

        public PosixMemoryMapPager(string file, long? initialFileSize = null)
        {
            _file = file;
            _fd = Syscall.open(file, OpenFlags.O_RDWR | OpenFlags.O_CREAT | OpenFlags.O_SYNC,
                              FilePermissions.S_IWUSR | FilePermissions.S_IRUSR);
            if (_fd == -1)
                PosixHelper.ThrowLastError(Marshal.GetLastWin32Error());

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

            NumberOfAllocatedPages = _totalAllocationSize / PageSize;
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
            Stat buf;
            var result = Syscall.fstat(_fd, out buf);
            if (result == -1)
                PosixHelper.ThrowLastError(Marshal.GetLastWin32Error());
            var fileSize = buf.st_size;
            return fileSize;
        }

        protected override string GetSourceName()
        {
            return "mmap: " + _fd + " " + _file;
        }

        public override void AllocateMorePages(Transaction tx, long newLength)
        {
            ThrowObjectDisposedIfNeeded();
            var newLengthAfterAdjustment = NearestSizeToPageSize(newLength);

            if (newLengthAfterAdjustment <= _totalAllocationSize)
                return;

            var allocationSize = newLengthAfterAdjustment - _totalAllocationSize;

            PosixHelper.AllocateFileSpace(_fd, (ulong) (_totalAllocationSize + allocationSize));
            PagerState newPagerState = CreatePagerState();
            if (newPagerState == null)
            {
                var errorMessage = string.Format(
                    "Unable to allocate more pages - unsuccessfully tried to allocate continuous block of virtual memory with size = {0:##,###;;0} bytes",
                    (_totalAllocationSize + allocationSize));

                throw new OutOfMemoryException(errorMessage);
            }

            newPagerState.DebugVerify(newLengthAfterAdjustment);

            if (tx != null)
            {
                tx.EnsurePagerStateReference(newPagerState);
            }

            var tmp = PagerState;
            PagerState = newPagerState;
            tmp.Release(); //replacing the pager state --> so one less reference for it

            _totalAllocationSize += allocationSize;
            NumberOfAllocatedPages = _totalAllocationSize/PageSize;
        }


        private PagerState CreatePagerState()
        {
            var fileSize = GetFileSize();
            var startingBaseAddressPtr = Syscall.mmap(IntPtr.Zero, (ulong)fileSize,
                                                      MmapProts.PROT_READ | MmapProts.PROT_WRITE,
                                                      MmapFlags.MAP_SHARED, _fd, 0);

            if (startingBaseAddressPtr.ToInt64() == -1) //system didn't succeed in mapping the address where we wanted
                PosixHelper.ThrowLastError(Marshal.GetLastWin32Error());

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
            //TODO: Is it worth it to change to just one call for msync for the entire file?
            foreach (var alloc in PagerState.AllocationInfos)
            {
                var result = Syscall.msync(new IntPtr(alloc.BaseAddress), (ulong)alloc.Size, MsyncFlags.MS_SYNC);
                if (result == -1)
                    PosixHelper.ThrowLastError(Marshal.GetLastWin32Error());
            }
        }


        public override string ToString()
        {
            return _file;
        }

        public override void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            var result = Syscall.munmap(new IntPtr(baseAddress), (ulong)size);
            if (result == -1)
                PosixHelper.ThrowLastError(Marshal.GetLastWin32Error());
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
