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
		private readonly FlushMode _flushMode;
		private readonly FileStream _fileStream;
		private readonly IntPtr _fileHandle;
		private FileInfo _fileInfo;

		public FilePager(string file, FlushMode flushMode = FlushMode.Full)
		{
			_flushMode = flushMode;
			_fileInfo = new FileInfo(file);

			var noData = _fileInfo.Exists == false || _fileInfo.Length == 0;

			var safeHandle = NativeFileMethods.CreateFile(file,
			                                              NativeFileAccess.GenericRead | NativeFileAccess.GenericWrite,
			                                              NativeFileShare.Read, IntPtr.Zero,
			                                              NativeFileCreationDisposition.OpenAlways,
														  NativeFileAttributes.Normal, //TODO here we could pass Write_Through or (and) NoBuffering
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

		public override byte* AcquirePagePointer(long pageNumber)
		{
			return PagerState.Base + (pageNumber*PageSize);
		}

		public override Page GetWritable(long pageNumber)
		{
			throw new InvalidOperationException("File pager does not offer writing directly to a page");
		}

		public override void Flush(long start, long count)
		{
			// nothing to do here - this is needed for memory mapped files only
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
			if (_flushMode == FlushMode.Full)
				_fileStream.Flush(true);
		}

		public override void Write(Page page, long? pageNumber)
		{
		    var number = pageNumber ?? page.PageNumber;

			Debug.Assert(number <= NumberOfAllocatedPages);

		    var position = number * PageSize;

			var nativeOverlapped = new NativeOverlapped()
			{
				OffsetLow = (int)(position & 0xffffffff),
				OffsetHigh = (int)(position >> 32)
			};

			var toWrite = (uint)(page.IsOverflow ? (page.OverflowSize + Constants.PageHeaderSize) : PageSize);

            var startWrite = page.Base;
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

			if(DeleteOnClose)
				_fileInfo.Delete();
		}
	}
}
