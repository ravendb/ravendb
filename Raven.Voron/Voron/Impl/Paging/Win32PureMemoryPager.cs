using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Voron.Trees;

namespace Voron.Impl.Paging
{
	public unsafe class Win32PureMemoryPager : AbstractPager
	{
		private readonly byte* _baseAddress;
		private readonly ulong _reservedSize;
        private ulong _rangesCount;
        

		// ReSharper disable InconsistentNaming

		
		[Obsolete("Should not be used - needs to be phased away")]
		public Win32PureMemoryPager()
		{
			_reservedSize = Environment.Is64BitProcess ? 4 * 1024 * 1024 * 1024UL : 128 * 1024 * 1024UL;
			var dwSize = new UIntPtr(_reservedSize);
			_baseAddress = NativeMethods.VirtualAlloc(null, dwSize, NativeMethods.AllocationType.RESERVE, NativeMethods.MemoryProtection.NOACCESS);
			if (_baseAddress == null)
				throw new Win32Exception();
		    _rangesCount = 1;
		}

		[Obsolete("Should not be used - needs to be phased away")]
		public Win32PureMemoryPager(byte[] data)
			: this()
		{
			AllocatePages(data.Length / PageSize + (data.Length % PageSize == 0 ? 0 : 1));
			fixed (byte* origin = data)
			{
				NativeMethods.memcpy(_baseAddress, origin, data.Length);
			}
		}

		public override byte* AcquirePagePointer(long pageNumber, PagerState pagerState = null)
		{
			if (pageNumber >= NumberOfAllocatedPages)
				throw new InvalidOperationException("Tried to read a page that wasn't committed");
			return _baseAddress + (pageNumber * PageSize);
		}

		public override void AllocateMorePages(Transaction tx, long newLength)
		{
			var totalPages = newLength / PageSize;
			Debug.Assert(newLength % PageSize == 0);
			AllocatePages(totalPages - NumberOfAllocatedPages);
		}

		private void AllocatePages(long pagesToAdd)
		{
			var dwSize = (ulong)((pagesToAdd + NumberOfAllocatedPages) * PageSize);
			var lpAddress = _baseAddress + (NumberOfAllocatedPages * PageSize);

			if (lpAddress + dwSize > _baseAddress + _reservedSize)
			{
                var extResult = NativeMethods.VirtualAlloc(_baseAddress + _reservedSize, new UIntPtr(_reservedSize), NativeMethods.AllocationType.RESERVE, NativeMethods.MemoryProtection.NOACCESS);
			    if (extResult == null)
			        throw new InvalidOperationException(
			            "Tried to allocated pages beyond the reserved space of: " + _reservedSize +
			            " and could not grow the memory pager", new Win32Exception());
			    _rangesCount++;
			}

			var result = NativeMethods.VirtualAlloc(lpAddress, new UIntPtr(dwSize), NativeMethods.AllocationType.COMMIT, NativeMethods.MemoryProtection.READWRITE);
			if (result == null)
				throw new Win32Exception();

			NumberOfAllocatedPages += pagesToAdd;

		}

		protected override string GetSourceName()
		{
			return "Win32PureMemoryPager";
		}

		public override void Sync()
		{
			// nothing to do here
		}

		public override string ToString()
		{
			return "memory";
		}

		public override int Write(Page page, long? pageNumber)
		{
			var toWrite = page.IsOverflow ? GetNumberOfOverflowPages(page.OverflowSize) : 1;
			var requestedPageNumber = pageNumber ?? page.PageNumber;

			return WriteDirect(page, requestedPageNumber, toWrite);
		}

		public override int WriteDirect(Page start, long pagePosition, int pagesToWrite)
		{
			EnsureContinuous(null, pagePosition, pagesToWrite);

			var toCopy = pagesToWrite*PageSize;
			NativeMethods.memcpy(AcquirePagePointer(pagePosition), start.Base, toCopy);

			return toCopy;
		}

		public override void Dispose()
		{
		    if (Disposed)
		        return;
			base.Dispose();
		    for (ulong i = 0; i < _rangesCount; i++)
		    {
                NativeMethods.VirtualFree(_baseAddress +(i * _reservedSize), new UIntPtr(_reservedSize), NativeMethods.FreeType.MEM_RELEASE);
		    }
		}
	}
}