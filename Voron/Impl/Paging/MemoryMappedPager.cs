using System;
using System.ComponentModel;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using Voron.Trees;

namespace Voron.Impl
{
	public unsafe class MemoryMapPager : AbstractPager
	{
		private readonly FileInfo _fileInfo;
	    private readonly SafeFileHandle _handle;
	    private long _length;
	    private FileStream _fileStream;

	    [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        static extern bool FlushFileBuffers(SafeFileHandle hFile);

        public MemoryMapPager(string file, NativeFileAttributes options = NativeFileAttributes.Normal)
		{
			_fileInfo = new FileInfo(file);
			var noData = _fileInfo.Exists == false || _fileInfo.Length == 0;
            _handle = NativeFileMethods.CreateFile(file, NativeFileAccess.GenericAll, NativeFileShare.Read, IntPtr.Zero,
                NativeFileCreationDisposition.OpenAlways, options, IntPtr.Zero);
	        if (_handle.IsInvalid)
	        {
		        var lastWin32ErrorCode = Marshal.GetLastWin32Error();
		        throw new IOException("Failed to open file storage of MemoryMapPager",new Win32Exception(lastWin32ErrorCode));
	        }

	        _fileStream = new FileStream(_handle, FileAccess.ReadWrite);

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
            if (newLength < _length)
				throw new ArgumentException("Cannot set the legnth to less than the current length");

            if (newLength == _length)
				return;

			// need to allocate memory again
			NativeFileMethods.SetFileLength(_handle, newLength);
		    _length = newLength;
			PagerState.Release(); // when the last transaction using this is over, will dispose it
			PagerState newPager = CreateNewPagerState();

			if (tx != null) // we only pass null during startup, and we don't need it there
			{
				newPager.AddRef(); // one for the current transaction
				tx.AddPagerState(newPager);
			}

			PagerState = newPager;
			NumberOfAllocatedPages = newLength / PageSize;
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
             FlushFileBuffers(_handle);
		}

		public override void Write(Page page, long? pageNumber)
		{
		    var startPage = pageNumber ?? page.PageNumber;

			var toWrite = page.IsOverflow ? GetNumberOfOverflowPages(page.OverflowSize) : 1;

			WriteDirect(page, startPage, toWrite);
		}

	    public override string ToString()
	    {
	        return _fileInfo.Name;
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
			_handle.Close();
			if(DeleteOnClose)
				_fileInfo.Delete();
		}
	}
}
