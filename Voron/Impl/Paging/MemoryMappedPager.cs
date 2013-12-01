using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using Voron.Trees;

namespace Voron.Impl.Paging
{
	public unsafe class MemoryMapPager : AbstractPager
	{
	    private readonly NativeFileAccess access;
		private readonly FileInfo _fileInfo;
	    private readonly SafeFileHandle _handle;
	    private long _length;
	    private FileStream _fileStream;

	    [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        static extern bool FlushFileBuffers(SafeFileHandle hFile);

        public MemoryMapPager(string file,
            bool asyncPagerRelease,
			NativeFileAttributes options = NativeFileAttributes.Normal,
			NativeFileAccess access = NativeFileAccess.GenericAll)
            : base(asyncPagerRelease)
		{
            this.access = access;
	        _fileInfo = new FileInfo(file);
			var noData = _fileInfo.Exists == false || _fileInfo.Length == 0;
			_handle = NativeFileMethods.CreateFile(file, access, NativeFileShare.Read | NativeFileShare.Write | NativeFileShare.Delete, IntPtr.Zero,
                NativeFileCreationDisposition.OpenAlways, options, IntPtr.Zero);
            if (_handle.IsInvalid)
                throw new Win32Exception();

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

			Debug.Assert(_fileStream.Length == newLength);

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
			var memoryMappedFileAccess = access == NativeFileAccess.GenericRead ? MemoryMappedFileAccess.Read : MemoryMappedFileAccess.ReadWrite;
			var mmf = MemoryMappedFile.CreateFromFile(_fileStream, Guid.NewGuid().ToString(), _fileStream.Length,
				memoryMappedFileAccess,
				null, HandleInheritability.None, true);
			var accessor = mmf.CreateViewAccessor(0, _fileStream.Length, memoryMappedFileAccess);
			byte* p = null;
			accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref p);

			var newPager = new PagerState(this, AsyncPagerRelease)
			{
				Accessor = accessor,
				File = mmf,
				MapBase = p
			};
			newPager.AddRef(); // one for the pager
			return newPager;
		}

		protected override unsafe string GetSourceName()
		{
			if (_fileInfo == null)
				return "Unknown";
			return "MemMap: " + _fileInfo.Name;
		}

		public override byte* AcquirePagePointer(long pageNumber)
		{
			return PagerState.MapBase + (pageNumber * PageSize);
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
            NativeMethods.memcpy(PagerState.MapBase + pagePosition * PageSize, start.Base, pagesToWrite * PageSize);
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
