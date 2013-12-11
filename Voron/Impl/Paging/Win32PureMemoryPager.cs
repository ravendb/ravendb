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

		[DllImport("kernel32.dll", SetLastError = true)]
		public static extern byte* VirtualAlloc(byte* lpAddress, UIntPtr dwSize,
		   AllocationType flAllocationType, MemoryProtection flProtect);

		[DllImport("kernel32.dll", SetLastError = true)]
		public static extern bool VirtualFree(byte* lpAddress, UIntPtr dwSize,
		   FreeType dwFreeType);

		[Flags]
		public enum FreeType : uint
		{
			MEM_DECOMMIT = 0x4000,
			MEM_RELEASE = 0x8000
		}

		[Flags]
		public enum AllocationType : uint
		{
			COMMIT = 0x1000,
			RESERVE = 0x2000,
			RESET = 0x80000,
			LARGE_PAGES = 0x20000000,
			PHYSICAL = 0x400000,
			TOP_DOWN = 0x100000,
			WRITE_WATCH = 0x200000
		}

		[Flags]
		public enum MemoryProtection : uint
		{
			EXECUTE = 0x10,
			EXECUTE_READ = 0x20,
			EXECUTE_READWRITE = 0x40,
			EXECUTE_WRITECOPY = 0x80,
			NOACCESS = 0x01,
			READONLY = 0x02,
			READWRITE = 0x04,
			WRITECOPY = 0x08,
			GUARD_Modifierflag = 0x100,
			NOCACHE_Modifierflag = 0x200,
			WRITECOMBINE_Modifierflag = 0x400
		}
		// ReSharper restore InconsistentNaming

		public Win32PureMemoryPager()
		{
			_reservedSize = Environment.Is64BitProcess ? 4 * 1024 * 1024 * 1024UL : 128 * 1024 * 1024UL;
			var dwSize = new UIntPtr(_reservedSize);
			_baseAddress = VirtualAlloc(null, dwSize, AllocationType.RESERVE, MemoryProtection.NOACCESS);
			if (_baseAddress == null)
				throw new Win32Exception();
		    _rangesCount = 1;
		}

		public Win32PureMemoryPager(byte[] data)
			: this()
		{
			AllocatePages(data.Length / PageSize + (data.Length % PageSize == 0 ? 0 : 1));
			fixed (byte* origin = data)
			{
				NativeMethods.memcpy(_baseAddress, origin, data.Length);
			}
		}

		public override byte* AcquirePagePointer(long pageNumber)
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
                var extResult = VirtualAlloc(_baseAddress + _reservedSize, new UIntPtr(_reservedSize), AllocationType.RESERVE, MemoryProtection.NOACCESS);
			    if (extResult == null)
			        throw new InvalidOperationException(
			            "Tried to allocated pages beyond the reserved space of: " + _reservedSize +
			            " and could not grow the memory pager", new Win32Exception());
			    _rangesCount++;
			}

			var result = VirtualAlloc(lpAddress, new UIntPtr(dwSize), AllocationType.COMMIT, MemoryProtection.READWRITE);
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

		public override void Write(Page page, long? pageNumber)
		{
			var toWrite = page.IsOverflow ? GetNumberOfOverflowPages(page.OverflowSize) : 1;
			var requestedPageNumber = pageNumber ?? page.PageNumber;

			WriteDirect(page, requestedPageNumber, toWrite);
		}

		public override void WriteDirect(Page start, long pagePosition, int pagesToWrite)
		{
			EnsureContinuous(null, pagePosition, pagesToWrite);
			NativeMethods.memcpy(AcquirePagePointer(pagePosition), start.Base, pagesToWrite * PageSize);
		}

		public override void Dispose()
		{
			base.Dispose();
		    for (ulong i = 0; i < _rangesCount; i++)
		    {
                VirtualFree(_baseAddress +(i * _reservedSize), new UIntPtr(_reservedSize), FreeType.MEM_RELEASE);
		    }
		}
	}
}