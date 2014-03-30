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
        private readonly FileInfo _fileInfo;
        private readonly FileStream _fileStream;
        private readonly SafeFileHandle _handle;
        private readonly NativeFileAccess _access;
        private long _length;

        public Win32MemoryMapPager(string file,
            NativeFileAttributes options = NativeFileAttributes.Normal,
            NativeFileAccess access = NativeFileAccess.GenericRead | NativeFileAccess.GenericWrite)
        {
            _access = access;
            _fileInfo = new FileInfo(file);
            bool noData = _fileInfo.Exists == false || _fileInfo.Length == 0;
            _handle = NativeFileMethods.CreateFile(file, access,
                NativeFileShare.Read | NativeFileShare.Write | NativeFileShare.Delete, IntPtr.Zero,
                NativeFileCreationDisposition.OpenAlways, options, IntPtr.Zero);
            if (_handle.IsInvalid)
            {
                int lastWin32ErrorCode = Marshal.GetLastWin32Error();
                throw new IOException("Failed to open file storage of Win32MemoryMapPager",
                    new Win32Exception(lastWin32ErrorCode));
            }

            _fileStream = new FileStream(_handle, FileAccess.ReadWrite);

            if (noData)
            {
                NumberOfAllocatedPages = 0;
            }
            else
            {
                NumberOfAllocatedPages = _fileInfo.Length/PageSize;
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
            NumberOfAllocatedPages = newLength/PageSize;
        }

        private PagerState CreateNewPagerState()
        {
            MemoryMappedFileAccess memoryMappedFileAccess = _access == NativeFileAccess.GenericRead
                ? MemoryMappedFileAccess.Read
                : MemoryMappedFileAccess.ReadWrite;
            MemoryMappedFile mmf = MemoryMappedFile.CreateFromFile(_fileStream, null, _fileStream.Length,
                memoryMappedFileAccess,
                null, HandleInheritability.None, true);
            MemoryMappedViewAccessor accessor = mmf.CreateViewAccessor(0, _fileStream.Length, memoryMappedFileAccess);
            byte* p = null;
            accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref p);			
            var newPager = new PagerState(this)
            {
                Accessor = accessor,
				Files = new[] { mmf },
                MapBase = p
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
            return (pagerState ?? PagerState).MapBase + (pageNumber*PageSize);
        }

        public override void Sync()
        {
            if (MemoryMapNativeMethods.FlushViewOfFile(PagerState.MapBase, new IntPtr(PagerState.Accessor.Capacity)) == false)
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
            int toCopy = pagesToWrite*PageSize;
            NativeMethods.memcpy(PagerState.MapBase + pagePosition*PageSize, start.Base, toCopy);

            return toCopy;
        }

        public override void Dispose()
        {
            base.Dispose();

            _fileStream.Dispose();
            _handle.Close();
            if (DeleteOnClose)
                _fileInfo.Delete();
        }
    }
}