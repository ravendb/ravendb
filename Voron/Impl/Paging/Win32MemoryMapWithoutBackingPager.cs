using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.ComponentModel;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Paging
{
	public unsafe class Win32MemoryMapWithoutBackingPager : AbstractPager
	{
		public readonly long AllocationGranularity;
		private long _totalAllocationSize;
		private readonly string _memoryName;
		private const int MaxAllocationRetries = 100;

		public Win32MemoryMapWithoutBackingPager(string memoryName = null)
		{
			NativeMethods.SYSTEM_INFO systemInfo;
			NativeMethods.GetSystemInfo(out systemInfo);

			if (!String.IsNullOrWhiteSpace(memoryName))
				_memoryName = memoryName;
			else
				_memoryName = Guid.NewGuid().ToString();

			AllocationGranularity = systemInfo.allocationGranularity;
			_totalAllocationSize = systemInfo.allocationGranularity;

			PagerState.Release();
			Debug.Assert(AllocationGranularity % PageSize == 0);
			NumberOfAllocatedPages = _totalAllocationSize / PageSize;
			PagerState = CreateInitialPagerState(memoryName, _totalAllocationSize, null);
		}


		protected override string GetSourceName()
		{
			return "MemMapInSystemPage: " + _memoryName;
		}

		public override byte* AcquirePagePointer(long pageNumber, PagerState pagerState = null)
		{
			return (pagerState ?? PagerState).MapBase + (pageNumber * PageSize);
		}

		public override void Sync()
		{
			//nothing to do
		}


		public override int Write(Page page, long? pageNumber)
		{
			long startPage = pageNumber ?? page.PageNumber;

			int toWrite = page.IsOverflow ? GetNumberOfOverflowPages(page.OverflowSize) : 1;

			return WriteDirect(page, startPage, toWrite);
		}

		public override void AllocateMorePages(Transaction tx, long newLength)
		{
			var newLengthAfterAdjustment = NearestSizeToAllocationGranularity(newLength);

			if (newLengthAfterAdjustment < _totalAllocationSize)
				throw new ArgumentException("Cannot set the legnth to less than the current length");

			if (newLengthAfterAdjustment == _totalAllocationSize)
				return;

			var allocationSize = NearestSizeToAllocationGranularity(newLength - _totalAllocationSize);

			if (!TryAllocateMoreContinuousPages(allocationSize))
			{
				var newPagerState = AllocateMorePagesAndRemapContinuously(allocationSize);
				if (newPagerState == null)
					throw new OutOfMemoryException(string.Format("Unable to allocate more pages - unsucsessfully tried to allocate continuous block of size = {0} bytes", (_totalAllocationSize + allocationSize)));
				
				newPagerState.AddRef();
				if (tx != null)
				{
					newPagerState.AddRef();
					tx.AddPagerState(newPagerState);
				}				
				PagerState = newPagerState;
			}

			_totalAllocationSize += allocationSize;
			NumberOfAllocatedPages = _totalAllocationSize / PageSize;
		}

	
		public override int WriteDirect(Page start, long pagePosition, int pagesToWrite)
		{
			int toCopy = pagesToWrite * PageSize;
			NativeMethods.memcpy(PagerState.MapBase + pagePosition * PageSize, start.Base, toCopy);

			return toCopy;
		}

		public override string ToString()
		{
			return GetSourceName() + ", Length: " + _totalAllocationSize;
		}

		private PagerState AllocateMorePagesAndRemapContinuously(long allocationSize)
		{
			var retryCount = 0;

			while (retryCount++ < MaxAllocationRetries)
			{
				byte* newBaseAddress;
				if (!TryFindContinuousMemory((ulong)(_totalAllocationSize + allocationSize), out newBaseAddress))
				{
					var message =
						string.Format(
							"Unable to allocate more pages - unsucsessfully tried to allocate continuous block of size = {0} bytes\r\n" +
							"It is likely that we are suffering from virtual memory exhaustion or memory fragmentation.\r\n" +
							"64 bits process: {1}\r\n" +
							"If you are running in 32 bits, this is expected, and you need to run in 64 bits to resume normal operations.\r\n" +
							"If you are running in 64 bits, this is likely an error and should be reported."
							, (_totalAllocationSize + allocationSize), Environment.Is64BitProcess);
					throw new OutOfMemoryException(message);
				}

				bool failedToAllocate = false;
				long offset = 0;
				var allocationInfoAfterReallocation = new List<PagerState.AllocationInfo>();
				foreach (var allocationInfo in PagerState.AllocationInfos)
				{
					var newAlloctedBaseAddress = MemoryMapNativeMethods.MapViewOfFileEx(allocationInfo.MappedFile.SafeMemoryMappedFileHandle.DangerousGetHandle(),
						MemoryMapNativeMethods.NativeFileMapAccessType.Read | MemoryMapNativeMethods.NativeFileMapAccessType.Write,
						0, 0,
						new UIntPtr((ulong)allocationInfo.Size),
						newBaseAddress + offset);

					if (newAlloctedBaseAddress == null || newAlloctedBaseAddress == (byte*)0)
					{
						// todo: log this out!
						UndoMappings(allocationInfoAfterReallocation);
						failedToAllocate = true;
						break;
					}

					offset += allocationInfo.Size;
					allocationInfoAfterReallocation.Add(new PagerState.AllocationInfo
					{
						BaseAddress = newAlloctedBaseAddress,
						MappedFile = allocationInfo.MappedFile,
						Size = allocationInfo.Size
					});
				}

				if (!failedToAllocate)
				{
					var newAllocationInfo = TryCreateNewFileMappingAtAddress(allocationSize, newBaseAddress + _totalAllocationSize);
					if (newAllocationInfo == null)
						continue;

					var newPagerState = new PagerState(this)
					{
						Files = PagerState.Files.Add(newAllocationInfo.MappedFile),
						AllocationInfos = PagerState.AllocationInfos.Add(newAllocationInfo),
						MapBase = PagerState.MapBase
					};
					return newPagerState;
				}
			}

			throw new InvalidOperationException("Something bad has happened, after " + MaxAllocationRetries + " tries, could not find any spot in virtual memory to " +
												"remap continuously memory");
		}

		private static void UndoMappings(IEnumerable<PagerState.AllocationInfo> newAllocationInfos)
		{
			foreach (var newAllocationInfo in newAllocationInfos)
				MemoryMapNativeMethods.UnmapViewOfFile(newAllocationInfo.BaseAddress);
		}

		private bool TryAllocateMoreContinuousPages(long allocationSize)
		{
			Debug.Assert(PagerState != null);
			Debug.Assert(PagerState.Files != null && PagerState.Files.Any());

			var allocationInfo = TryCreateNewFileMappingAtAddress(allocationSize, PagerState.MapBase);

			if (allocationInfo == null)
				return false;

			PagerState.Files = PagerState.Files.Add(allocationInfo.MappedFile);
			PagerState.AllocationInfos = PagerState.AllocationInfos.Add(allocationInfo);

			return true;
		}

		private PagerState.AllocationInfo TryCreateNewFileMappingAtAddress(long allocationSize, byte* baseAddress)
		{
			var newMemoryMappedFile = MemoryMappedFile.CreateNew(Guid.NewGuid().ToString(), allocationSize);
			var newFileMappingHandle = newMemoryMappedFile.SafeMemoryMappedFileHandle.DangerousGetHandle();
			var newMappingBaseAddress = MemoryMapNativeMethods.MapViewOfFileEx(newFileMappingHandle,
				MemoryMapNativeMethods.NativeFileMapAccessType.Read | MemoryMapNativeMethods.NativeFileMapAccessType.Write,
				0, 0,
				UIntPtr.Zero,
				baseAddress + _totalAllocationSize);

			var hasMappingSucceeded = newMappingBaseAddress != null && newMappingBaseAddress != (byte*)0;
			if (!hasMappingSucceeded)
			{
				newMemoryMappedFile.Dispose();
				return null;
			}

			return new PagerState.AllocationInfo
			{
				BaseAddress = newMappingBaseAddress,
				Size = allocationSize,
				MappedFile = newMemoryMappedFile
			};
		}


		private bool TryFindContinuousMemory(ulong size, out byte* foundAddressPtr)
		{
			foundAddressPtr = null;
			try
			{
				foundAddressPtr = NativeMethods.VirtualAlloc(null, new UIntPtr(size), NativeMethods.AllocationType.RESERVE,
					NativeMethods.MemoryProtection.READWRITE);

				return (foundAddressPtr != null && foundAddressPtr != (byte*)0);
			}
			finally
			{
				if (foundAddressPtr != null && foundAddressPtr != (byte*)0)
					NativeMethods.VirtualFree(foundAddressPtr, UIntPtr.Zero, NativeMethods.FreeType.MEM_RELEASE);
			}

		}
		
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private long NearestSizeToAllocationGranularity(long size)
		{
			if (size % AllocationGranularity == 0)
				return size;

			var ratio = Convert.ToInt64(Math.Ceiling((decimal)size / AllocationGranularity));
			return AllocationGranularity * ratio;
		}

		private PagerState CreateInitialPagerState(string memoryName, long size, byte* requestedBaseAddress)
		{
			var allocationSize = NearestSizeToAllocationGranularity(size);
			var mmf = MemoryMappedFile.CreateNew(memoryName, allocationSize, MemoryMappedFileAccess.ReadWrite);

			var fileMappingHandle = mmf.SafeMemoryMappedFileHandle.DangerousGetHandle();

			//TODO : do not forget to add error checking after CreateFileMapping / MemoryMappedFile.CreateNew

			var startingBaseAddressPtr = MemoryMapNativeMethods.MapViewOfFileEx(fileMappingHandle,
				MemoryMapNativeMethods.NativeFileMapAccessType.Read | MemoryMapNativeMethods.NativeFileMapAccessType.Write,
				0, 0,
				UIntPtr.Zero, //map all what was "reserved" in CreateFileMapping on previous row
				requestedBaseAddress);

			if (startingBaseAddressPtr == (byte*)0) //system didn't succeed in mapping the address where we wanted
				throw new Win32Exception();

			var allocationInfo = new PagerState.AllocationInfo
			{
				BaseAddress = startingBaseAddressPtr,
				Size = allocationSize,
				MappedFile = mmf
			};

			var newPager = new PagerState(this)
			{
				Files = (new[] { mmf }).ToImmutableList(),
				Accessor = null, //not available since MapViewOfFileEx is used (instead of MapViewOfFile - which is used in managed wrapper)
				MapBase = startingBaseAddressPtr,
				AllocationInfos = (new[] { allocationInfo }).ToImmutableList()
			};

			newPager.AddRef();

			return newPager;
		}
	}
}
