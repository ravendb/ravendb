using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using Voron.Trees;

namespace Voron.Impl
{
	public unsafe class MemoryMapPager : AbstractPager
	{
		private readonly FileStream _fileStream;
		private readonly FileInfo _fileInfo;

		[DllImport("kernel32.dll", SetLastError = true)]
		[return: MarshalAs(UnmanagedType.Bool)]
		extern static bool FlushViewOfFile(byte* lpBaseAddress, IntPtr dwNumberOfBytesToFlush);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        static extern bool FlushFileBuffers(SafeFileHandle hFile);

		public MemoryMapPager(string file)
		{
			_fileInfo = new FileInfo(file);
			var noData = _fileInfo.Exists == false || _fileInfo.Length == 0;
			_fileStream = _fileInfo.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
			if (noData)
			{
				NumberOfAllocatedPages = 0;
			}
			else
			{
				NumberOfAllocatedPages = _fileInfo.Length / PageSize;
				PagerState.Release();
				PagerState = CreateNewPagerState();
			}
		}

		public override void AllocateMorePages(Transaction tx, long newLength)
		{
			if (newLength < _fileStream.Length)
				throw new ArgumentException("Cannot set the legnth to less than the current length");

			if (newLength == _fileStream.Length)
				return;

			// need to allocate memory again
			_fileStream.SetLength(newLength);
			PagerState.Release(); // when the last transaction using this is over, will dispose it
			PagerState newPager = CreateNewPagerState();

			if (tx != null) // we only pass null during startup, and we don't need it there
			{
				newPager.AddRef(); // one for the current transaction
				tx.AddPagerState(newPager);
			}

			PagerState = newPager;
			NumberOfAllocatedPages = newPager.Accessor.Capacity / PageSize;
		}

		private PagerState CreateNewPagerState()
		{
			var mmf = MemoryMappedFile.CreateFromFile(_fileStream, Guid.NewGuid().ToString(), _fileStream.Length,
													  MemoryMappedFileAccess.ReadWrite, null, HandleInheritability.None, true);
			var accessor = mmf.CreateViewAccessor();
			byte* p = null;
			accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref p);

			var newPager = new PagerState
			{
				Accessor = accessor,
				File = mmf,
				Base = p
			};
			newPager.AddRef(); // one for the pager
			return newPager;
		}

		public override byte* AcquirePagePointer(long pageNumber)
		{
			return PagerState.Base + (pageNumber * PageSize);
		}

		public override void Sync()
		{
             FlushFileBuffers(_fileStream.SafeFileHandle);
		}

		public override void Write(Page page, long? pageNumber)
		{
		    var startPage = pageNumber ?? page.PageNumber;

			var toWrite = page.IsOverflow ? GetNumberOfOverflowPages(page.OverflowSize) : 1;

			WriteDirect(page, startPage, toWrite);
		}

	    public override void WriteDirect(Page start, long pagePosition, int pagesToWrite)
	    {
            NativeMethods.memcpy(PagerState.Base + pagePosition * PageSize, start.Base, pagesToWrite * PageSize);
	    }

	    public override void Dispose()
		{
			base.Dispose();
			if (PagerState != null)
			{
				PagerState.Release();
				PagerState = null;
			}
			_fileStream.Dispose();
			if(DeleteOnClose)
				_fileInfo.Delete();
		}
	}
}
