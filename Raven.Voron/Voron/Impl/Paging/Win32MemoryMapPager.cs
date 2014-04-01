using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Voron.Impl.Paging
{
	using Microsoft.Win32.SafeHandles;
	using System;
	using System.ComponentModel;
	using System.Diagnostics;
	using System.IO;
	using System.IO.MemoryMappedFiles;
	using System.Runtime.InteropServices;
	using Voron.Trees;
	using Voron.Util;

	public unsafe class Win32MemoryMapPager : AbstractPager
	{
		public readonly long AllocationGranularity;
		private long _totalAllocationSize;
		private readonly FileInfo _fileInfo;
		private readonly FileStream _fileStream;
		private readonly SafeFileHandle _handle;
		private readonly NativeFileAccess _access;
		private readonly MemoryMappedFileAccess _memoryMappedFileAccess;

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
			NativeFileAttributes options = NativeFileAttributes.Normal,
			NativeFileAccess access = NativeFileAccess.GenericRead | NativeFileAccess.GenericWrite)
		{
			NativeMethods.SYSTEM_INFO systemInfo;
			NativeMethods.GetSystemInfo(out systemInfo);

			AllocationGranularity = systemInfo.allocationGranularity;

			_access = access;
			_memoryMappedFileAccess = _access == NativeFileAccess.GenericRead
				? MemoryMappedFileAccess.Read
				: MemoryMappedFileAccess.ReadWrite;

			_handle = NativeFileMethods.CreateFile(file, access,
			   NativeFileShare.Read | NativeFileShare.Write | NativeFileShare.Delete, IntPtr.Zero,
			   NativeFileCreationDisposition.OpenAlways, options, IntPtr.Zero);
			if (_handle.IsInvalid)
			{
				int lastWin32ErrorCode = Marshal.GetLastWin32Error();
				throw new IOException("Failed to open file storage of Win32MemoryMapPager",
					new Win32Exception(lastWin32ErrorCode));
			}

			_fileInfo = new FileInfo(file);

			var streamAccessType = _access == NativeFileAccess.GenericRead
				? FileAccess.Read
				: FileAccess.ReadWrite;
			_fileStream = new FileStream(_handle, streamAccessType);

			_totalAllocationSize = _fileInfo.Length;

			if (_access.HasFlag(NativeFileAccess.GenericWrite) || 
				_access.HasFlag(NativeFileAccess.GenericAll) ||
				_access.HasFlag(NativeFileAccess.FILE_GENERIC_WRITE))
			{
				long fileLengthAfterAdjustment = _fileStream.Length;
				if (_fileStream.Length == 0 || (_fileStream.Length%AllocationGranularity != 0))
				{
					fileLengthAfterAdjustment = NearestSizeToAllocationGranularity(_fileInfo.Length);
					_fileStream.SetLength(fileLengthAfterAdjustment);
				}

				_totalAllocationSize = fileLengthAfterAdjustment;
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

			if (newLengthAfterAdjustment < _totalAllocationSize)
				throw new ArgumentException("Cannot set the legnth to less than the current length");

			if (newLengthAfterAdjustment == _totalAllocationSize)
				return;

			var allocationSize = newLengthAfterAdjustment - _totalAllocationSize;

			if (_totalAllocationSize + allocationSize >= long.MaxValue) //probably would never be true, but just in case
				throw new OutOfMemoryException("failed to allocated more pages - reached maximum allowed space usage");

			_fileStream.SetLength(_totalAllocationSize + allocationSize);
			if (TryAllocateMoreContinuousPages(allocationSize) == false)
			{
				PagerState newPagerState = CreatePagerState();
				if (newPagerState == null)
				{
					var errorMessage = string.Format(
						"Unable to allocate more pages - unsucsessfully tried to allocate continuous block of virtual memory with size = {0:##,###;;0} bytes",
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
			}

			_totalAllocationSize += allocationSize;
			NumberOfAllocatedPages = _totalAllocationSize / PageSize;
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

			return true;
		}

		private PagerState.AllocationInfo RemapViewOfFileAtAddress(long allocationSize, ulong offsetInFile, byte* baseAddress)
		{
			var offset = new SplitValue { Value = offsetInFile };

			var mmf = MemoryMappedFile.CreateFromFile(_fileStream, null, _fileStream.Length,
				_memoryMappedFileAccess,
				null, HandleInheritability.None, true);

			var newMappingBaseAddress = MemoryMapNativeMethods.MapViewOfFileEx(mmf.SafeMemoryMappedFileHandle.DangerousGetHandle(),
				MemoryMapNativeMethods.NativeFileMapAccessType.Read | MemoryMapNativeMethods.NativeFileMapAccessType.Write,
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
			var mmFileAccessType = _access == NativeFileAccess.GenericRead
				? MemoryMapNativeMethods.NativeFileMapAccessType.Read
				: MemoryMapNativeMethods.NativeFileMapAccessType.Read | MemoryMapNativeMethods.NativeFileMapAccessType.Write;

			var startingBaseAddressPtr = MemoryMapNativeMethods.MapViewOfFileEx(fileMappingHandle,
				mmFileAccessType,
				0, 0,
				UIntPtr.Zero, //map all what was "reserved" in CreateFileMapping on previous row
				null);


			if (startingBaseAddressPtr == (byte*)0) //system didn't succeed in mapping the address where we wanted
				throw new Win32Exception();

			var allocationInfo = new PagerState.AllocationInfo
			{
				BaseAddress = startingBaseAddressPtr,
				Size = _fileStream.Length,
				MappedFile = mmf
			};

			var newPager = new PagerState(this)
			{
				Files = new[] { mmf },
				Accessor = null, //not available since MapViewOfFileEx is used (instead of MapViewOfFile - which is used in managed wrapper)
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

		public override byte* AcquirePagePointer(long pageNumber, PagerState pagerState = null)
		{
			ThrowObjectDisposedIfNeeded();

			return (pagerState ?? PagerState).MapBase + (pageNumber*PageSize);
		}

		public override void Sync()
		{
			ThrowObjectDisposedIfNeeded();

			if (PagerState.AllocationInfos.Any(allocationInfo => 
				MemoryMapNativeMethods.FlushViewOfFile(allocationInfo.BaseAddress, new IntPtr(allocationInfo.Size)) == false))
					throw new Win32Exception();

			if (MemoryMapNativeMethods.FlushFileBuffers(_handle) == false)
				throw new Win32Exception();
		}

		public override int Write(Page page, long? pageNumber)
		{
			long startPage = pageNumber ?? page.PageNumber;

			int toWrite = page.IsOverflow ? GetNumberOfOverflowPages(page.OverflowSize) : 1;

			return WriteDirect(page, startPage, toWrite);
		}

		public override string ToString()
		{
			return _fileInfo.Name;
		}

		public override int WriteDirect(Page start, long pagePosition, int pagesToWrite)
		{
			ThrowObjectDisposedIfNeeded();

			int toCopy = pagesToWrite*PageSize;
			NativeMethods.memcpy(PagerState.MapBase + pagePosition*PageSize, start.Base, toCopy);

			return toCopy;
		}

		public override void Dispose()
		{
		    if (Disposed)
		        return;

			_fileStream.Dispose();
			_handle.Close();
			if (DeleteOnClose)
				_fileInfo.Delete();

			base.Dispose();
		}

	}
}