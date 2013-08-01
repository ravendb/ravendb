using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using Nevar.Trees;

namespace Nevar.Impl
{
    public unsafe class MemoryMapPager : AbstractPager
    {
        private readonly FlushMode _flushMode;
        private long _allocatedPages;
        private readonly FileStream _fileStream;

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

        protected override void AllocateMorePages(Transaction tx, long newLength)
        {
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

        public override void Flush()
        {
            if (_flushMode == FlushMode.None)
                return;

            PagerState.Accessor.Flush();
            _fileStream.Flush(_flushMode == FlushMode.Full);
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