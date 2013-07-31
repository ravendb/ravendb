using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using Nevar.Trees;

namespace Nevar.Impl
{
    public unsafe class MemoryMapPager : AbstractPager
    {
        private long _allocatedPages;
        private readonly FileStream _fileStream;
      
        public MemoryMapPager(string file)
        {
            var fileInfo = new FileInfo(file);
            if (fileInfo.Exists == false || file.Length == 0)
            {
                _allocatedPages = 0;
                fileInfo.Create().Close();
            }
            else
            {
                _allocatedPages = file.Length / PageSize;
            }
            _fileStream = fileInfo.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
        }

	    protected override Page Get(long n)
        {
	        return new Page(PagerState.Base + (n * PageSize), PageMaxSpace);
        }

	    protected override void AllocateMorePages(Transaction tx, long newLength)
	    {
		    // need to allocate memory again
			_fileStream.SetLength(newLength);
		    var mmf = MemoryMappedFile.CreateFromFile(_fileStream, Guid.NewGuid().ToString(), _fileStream.Length,
		                                              MemoryMappedFileAccess.ReadWrite, null, HandleInheritability.None, true);
            PagerState.Release(); // when the last transaction using this is over, will dispose it

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

			if (tx != null) // we only pass null during startup, and we don't need it there
			{
				newPager.AddRef(); // one for the current transaction
                tx.AddPagerState(newPager);
			}

            PagerState = newPager;
		    _allocatedPages = accessor.Capacity/PageSize;
	    }

	    public override void Flush()
        {
            PagerState.Accessor.Flush();
            _fileStream.Flush(true);
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