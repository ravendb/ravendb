using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using Nevar.Trees;

namespace Nevar.Impl
{
    public unsafe class MemoryMapPager : AbstractPager
    {
        private readonly FlushMode _flushMode;
        private long _allocatedPages;
        private readonly FileStream _fileStream;

		[DllImport("kernel32.dll", SetLastError = true)]
		[return: MarshalAs(UnmanagedType.Bool)]
		extern static bool FlushViewOfFile(byte* lpBaseAddress, IntPtr dwNumberOfBytesToFlush);


        public MemoryMapPager(string file, FlushMode flushMode = FlushMode.Full)
        {
            _flushMode = flushMode;
            var fileInfo = new FileInfo(file);
            var hasData = fileInfo.Exists == false || fileInfo.Length == 0;
            _fileStream = fileInfo.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
            if (hasData)
            {
                _allocatedPages = 0;
            }
            else
            {
                _allocatedPages = fileInfo.Length / PageSize;
                PagerState.Release();
                PagerState = CreateNewPagerState();
            }
        }

        protected override Page Get(long n)
        {
            return new Page(PagerState.Base + (n * PageSize), PageMaxSpace);
        }

	    public override void AllocateMorePages(Transaction tx, long newLength)
	    {
		    if (newLength <= _fileStream.Length)
			    throw new ArgumentException("Cannot set the legnth to less than the current length");

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
            _allocatedPages = newPager.Accessor.Capacity / PageSize;
        }

        private PagerState CreateNewPagerState()
        {
            var mmf = MemoryMappedFile.CreateFromFile(_fileStream, Guid.NewGuid().ToString(), _fileStream.Length,
                                                      MemoryMappedFileAccess.ReadWrite, null, HandleInheritability.None, true);
            var accessor = mmf.CreateViewAccessor();
            byte* p = null;
            accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref p);

           var  newPager = new PagerState
                {
                    Accessor = accessor,
                    File = mmf,
                    Base = p
                };
            newPager.AddRef(); // one for the pager
            return newPager;
        }

		public override void Flush(List<long> sortedPagesToFlush)
        {
            if (_flushMode == FlushMode.None || sortedPagesToFlush.Count == 0)
                return;

			// here we try to optimize the amount of work we do, we will only 
			// flush the actual dirty pages, and we will do so in seqeuqntial order
			// ideally, this will save the OS the trouble of actually having to flush the 
			// entire range
			long start = sortedPagesToFlush[0];
			int count = 1;
			for (int i = 1; i < sortedPagesToFlush.Count; i++)
			{
				if (start + i != sortedPagesToFlush[i])
				{
					FlushPages(start, count);
					start = sortedPagesToFlush[i];
					count = 1;
				}
			}
			FlushPages(start, count);

			if (_flushMode == FlushMode.Full)
				_fileStream.Flush(true);
        }

		private void FlushPages(long startPage, int count)
		{
			//Console.WriteLine("Flushing {0,8:#,#} pages from {1,8:#,#}", count, count);
			FlushViewOfFile(PagerState.Base + (startPage*PageSize), new IntPtr(count*PageSize));
		}

        public override void Dispose()
        {
            if (PagerState != null)
            {
                PagerState.Release();
                PagerState = null;
            }
            _fileStream.Dispose();
        }

        public override long NumberOfAllocatedPages { get { return _allocatedPages; } }
    }
}