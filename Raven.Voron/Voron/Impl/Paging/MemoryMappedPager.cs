namespace Voron.Impl.Paging
{
	using System;
	using System.ComponentModel;
	using System.Diagnostics;
	using System.IO;
	using System.IO.MemoryMappedFiles;
	using System.Runtime.InteropServices;

	using Microsoft.Win32.SafeHandles;

	using Voron.Trees;
	using Voron.Util;

	public unsafe class Win32MemoryMapPager : AbstractPager
    {
        private readonly NativeFileAccess access;
        private readonly FileInfo _fileInfo;
        private readonly SafeFileHandle _handle;
        private long _length;
        private readonly FileStream _fileStream;

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        static extern bool FlushFileBuffers(SafeFileHandle hFile);


        [DllImport("kernel32.dll")]
        static extern bool FlushViewOfFile(byte* lpBaseAddress, IntPtr dwNumberOfBytesToFlush);

        [DllImport("kernel32.dll")]
        static extern IntPtr GetCurrentProcess();

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        static extern bool PrefetchVirtualMemory(IntPtr hProcess, UIntPtr NumberOfEntries, WIN32_MEMORY_RANGE_ENTRY* VirtualAddresses, ulong Flags);

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
        private class MEMORYSTATUSEX
        {
            public uint dwLength;
            public uint dwMemoryLoad;
            public ulong ullTotalPhys;
            public ulong ullAvailPhys;
            public ulong ullTotalPageFile;
            public ulong ullAvailPageFile;
            public ulong ullTotalVirtual;
            public ulong ullAvailVirtual;
            public ulong ullAvailExtendedVirtual;
            public MEMORYSTATUSEX()
            {
                this.dwLength = (uint)Marshal.SizeOf(typeof(MEMORYSTATUSEX));
            }
        }


        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        static extern bool GlobalMemoryStatusEx([In, Out] MEMORYSTATUSEX lpBuffer);

        static readonly Version win8version = new Version(6, 2, 9200, 0);

        [StructLayout(LayoutKind.Sequential)]
        private struct WIN32_MEMORY_RANGE_ENTRY
        {
            public byte* VirtualAddress;
            public IntPtr NumberOfBytes;
        }

        public Win32MemoryMapPager(string file,
            NativeFileAttributes options = NativeFileAttributes.Normal,
            NativeFileAccess access = NativeFileAccess.GenericRead | NativeFileAccess.GenericWrite)
        {
            this.access = access;
            _fileInfo = new FileInfo(file);
            var noData = _fileInfo.Exists == false || _fileInfo.Length == 0;
            _handle = NativeFileMethods.CreateFile(file, access, NativeFileShare.Read | NativeFileShare.Write | NativeFileShare.Delete, IntPtr.Zero,
                NativeFileCreationDisposition.OpenAlways, options, IntPtr.Zero);
	        if (_handle.IsInvalid)
	        {
		        var lastWin32ErrorCode = Marshal.GetLastWin32Error();
		        throw new IOException("Failed to open file storage of Win32MemoryMapPager",new Win32Exception(lastWin32ErrorCode));
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

                TryPrefetchingData();
            }
        }

        private void TryPrefetchingData()
        {
            if (Environment.OSVersion.Platform != PlatformID.Win32NT || Environment.OSVersion.Version < win8version)
                return; // this is limited to windows 8 or higher
            var lpBuffer = new MEMORYSTATUSEX();
            if (GlobalMemoryStatusEx(lpBuffer) == false)
                return;

            if (lpBuffer.dwMemoryLoad > 75)
                return; // system loaded, let just load on demand

            var size = Math.Min(_fileInfo.Length, (long)lpBuffer.ullAvailPhys);

            var entries = stackalloc WIN32_MEMORY_RANGE_ENTRY[1];
            entries[0].NumberOfBytes = new IntPtr(size);
            entries[0].VirtualAddress = PagerState.MapBase;

            if(PrefetchVirtualMemory(GetCurrentProcess(), new UIntPtr(1), entries, 0)== false)
                throw new Win32Exception();
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
            var mmf = MemoryMappedFile.CreateFromFile(_fileStream, null, _fileStream.Length,
                memoryMappedFileAccess,
                null, HandleInheritability.None, true);
            var accessor = mmf.CreateViewAccessor(0, _fileStream.Length, memoryMappedFileAccess);
            byte* p = null;
            accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref p);

            var newPager = new PagerState(this)
            {
                Accessor = accessor,
                File = mmf,
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
            return (pagerState ?? PagerState).MapBase + (pageNumber * PageSize);
        }

        public override void Sync()
        {
            if(FlushViewOfFile(PagerState.MapBase, new IntPtr(PagerState.Accessor.Capacity)) == false)
                    throw new Win32Exception();
            if (FlushFileBuffers(_handle) == false)
                throw new Win32Exception();
        }

        public override int Write(Page page, long? pageNumber)
        {
            var startPage = pageNumber ?? page.PageNumber;

            var toWrite = page.IsOverflow ? GetNumberOfOverflowPages(page.OverflowSize) : 1;

            return WriteDirect(page, startPage, toWrite);
        }

        public override string ToString()
        {
            return _fileInfo.Name;
        }

        public override int WriteDirect(Page start, long pagePosition, int pagesToWrite)
        {
	        var toCopy = pagesToWrite*PageSize;
            NativeMethods.memcpy(PagerState.MapBase + pagePosition * PageSize, start.Base, toCopy);

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
