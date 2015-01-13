using System;
using System.Runtime.InteropServices;
using System.Threading;
using Mono.Unix.Native;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Trees;
using Voron.Util;

namespace Voron.Platform.Posix
{
	public unsafe class PosixPageFileBackedMemoryMapPager : AbstractPager
	{
		private readonly string _file;
		private int _fd;
		public readonly long SysPageSize;
		private long _totalAllocationSize;
		private static int _counter;

		public PosixPageFileBackedMemoryMapPager(string file, long? initialFileSize = null)
		{
			var instanceId = Interlocked.Increment(ref _counter);
			_file = "/" + Syscall.getpid() + "-" + instanceId + "-" + file;
			_fd = Rt.shm_open(_file, OpenFlags.O_RDWR | OpenFlags.O_CREAT, (int)FilePermissions.ALLPERMS);
			if (_fd == -1)
				PosixHelper.ThrowLastError(Marshal.GetLastWin32Error());

			SysPageSize = Syscall.sysconf(SysconfName._SC_PAGESIZE);

			if (initialFileSize.HasValue)
			{
				_totalAllocationSize = NearestSizeToPageSize(initialFileSize.Value);
			}

			_totalAllocationSize = NearestSizeToPageSize(_totalAllocationSize);
			var result = Syscall.ftruncate (_fd, _totalAllocationSize);
			if (result != 0)
				PosixHelper.ThrowLastError (result);

			NumberOfAllocatedPages = _totalAllocationSize / PageSize;
			PagerState.Release();
			PagerState = CreatePagerState();
		}

		private long NearestSizeToPageSize(long size)
		{
			if (size == 0)
				return SysPageSize * 16;

			var mod = size%SysPageSize;
			if (mod == 0)
			{
				return size;
			}
			return ((size/SysPageSize) + 1)*SysPageSize;
		}

		protected override string GetSourceName()
		{
			return "shm_open mmap: " + _fd + " " + _file;
		}

		public override void AllocateMorePages(Transaction tx, long newLength)
		{
			ThrowObjectDisposedIfNeeded();

			var newLengthAfterAdjustment = NearestSizeToPageSize(newLength);

			if (newLengthAfterAdjustment <= _totalAllocationSize) //nothing to do
				return;

			var allocationSize = newLengthAfterAdjustment - _totalAllocationSize;

			Syscall.ftruncate(_fd, _totalAllocationSize + allocationSize);
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

			if (tx != null)
			{
				newPagerState.AddRef();
				tx.AddPagerState(newPagerState);
			}

			var tmp = PagerState;
			PagerState = newPagerState;
			tmp.Release(); //replacing the pager state --> so one less reference for it

			NumberOfAllocatedPages = _totalAllocationSize / PageSize;
		}

		private PagerState CreatePagerState()
		{
			var startingBaseAddressPtr = Syscall.mmap(IntPtr.Zero, (ulong)_totalAllocationSize,
			                                          MmapProts.PROT_READ | MmapProts.PROT_WRITE,
			                                          MmapFlags.MAP_SHARED, _fd, 0);

			if (startingBaseAddressPtr.ToInt64() == -1) //system didn't succeed in mapping the address where we wanted
				PosixHelper.ThrowLastError(Marshal.GetLastWin32Error());

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

			newPager.AddRef(); // one for the pager
			return newPager;
		}

		
		public override byte* AcquirePagePointer(long pageNumber, PagerState pagerState = null)
		{
			ThrowObjectDisposedIfNeeded();
			return (pagerState ?? PagerState).MapBase + (pageNumber * PageSize);
		}

		
		public override  void Sync()
		{
			//nothing to do here
		}


		public override int Write(Page page, long? pageNumber)
		{
			long startPage = pageNumber ?? page.PageNumber;

			int toWrite = page.IsOverflow ? GetNumberOfOverflowPages(page.OverflowSize) : 1;

			return WriteDirect(page, startPage, toWrite);
		}

		public override int WriteDirect(Page start, long pagePosition, int pagesToWrite)
		{
			ThrowObjectDisposedIfNeeded();

			int toCopy = pagesToWrite * PageSize;
            MemoryUtils.Copy(PagerState.MapBase + pagePosition * PageSize, start.Base, toCopy);

			return toCopy;
		}

		public override string ToString()
		{
			return _file;
		}

		public override void ReleaseAllocationInfo(byte* baseAddress, long size)
		{
			var result = Syscall.munmap(new IntPtr(baseAddress), (ulong) size);
			if (result == -1)
				PosixHelper.ThrowLastError(Marshal.GetLastWin32Error());
		}

		public override void Dispose ()
		{
			base.Dispose ();
			if (_fd != -1) 
			{
				Syscall.close (_fd);
				Rt.shm_unlink (_file);
				_fd = -1;
			}		
		}
	}
}

