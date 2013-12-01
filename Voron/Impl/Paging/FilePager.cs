using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Threading;
using Voron.Trees;

namespace Voron.Impl
{
    public unsafe class FilePager : AbstractPager
    {
        private readonly FileStream _fileStream;
        private readonly IntPtr _fileHandle;
        private readonly FileInfo _fileInfo;

        public FilePager(string file)
        {
            _fileInfo = new FileInfo(file);

            var noData = _fileInfo.Exists == false || _fileInfo.Length == 0;

            var safeHandle = NativeFileMethods.CreateFile(file,
                                                          NativeFileAccess.GenericRead | NativeFileAccess.GenericWrite,
                                                          NativeFileShare.Read, IntPtr.Zero,
                                                          NativeFileCreationDisposition.OpenAlways,
                                                          NativeFileAttributes.Write_Through | NativeFileAttributes.NoBuffering,
                                                          IntPtr.Zero);

            if (safeHandle.IsInvalid)
            {
                throw new Win32Exception();
            }

            _fileHandle = safeHandle.DangerousGetHandle();
            _fileStream = new FileStream(safeHandle, FileAccess.ReadWrite);

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

        public FileStream FileStream { get { return _fileStream; }}

        public override byte* AcquirePagePointer(long pageNumber)
        {
            return PagerState.Base + (pageNumber * PageSize);
        }

        public override Page GetWritable(long pageNumber)
        {
            throw new InvalidOperationException("File pager does not offer writing directly to a page");
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
                                                      MemoryMappedFileAccess.Read, null, HandleInheritability.None, true);

            MemoryMappedViewAccessor accessor;
            try
            {
                accessor = mmf.CreateViewAccessor(0, _fileStream.Length, MemoryMappedFileAccess.Read);
            }
            catch (Exception)
            {
                mmf.Dispose();
                throw;
            }
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

        public override void Sync()
        {
            _fileStream.Flush(true);
        }

        public override void Write(Page page, long? pageNumber)
        {
            var number = pageNumber ?? page.PageNumber;

            Debug.Assert(number <= NumberOfAllocatedPages);


            var toWrite = page.IsOverflow ? GetNumberOfOverflowPages(page.OverflowSize) : 1;

            WriteDirect(page, number, toWrite);
        }

        public override void WriteDirect(Page start, long pagePosition, int pagesToWrite)
        {
            var position = pagePosition * PageSize;
            var toWrite = (uint)(pagesToWrite * PageSize);

            var nativeOverlapped = new NativeOverlapped()
            {
                OffsetLow = (int)(position & 0xffffffff),
                OffsetHigh = (int)(position >> 32),
            };

            var startWrite = start.Base;
            while (toWrite != 0)
            {
                uint written;
                if (NativeFileMethods.WriteFile(_fileHandle, startWrite, toWrite, out written, ref nativeOverlapped) == false)
                {
                    throw new Win32Exception();
                }
                toWrite -= written;
                startWrite += written;
            }
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

            if (DeleteOnClose)
                _fileInfo.Delete();
        }
    }
}
