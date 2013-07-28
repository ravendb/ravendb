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
      
        private PagerState _pagerState;


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
            _pagerState = new PagerState();
        }

        public override Page Get(long n)
        {
            if (n >= _allocatedPages)
            {
                // need to allocate memory again
                _fileStream.SetLength(GetNewLength(_fileStream.Length));
                var mmf = MemoryMappedFile.CreateFromFile(_fileStream, Guid.NewGuid().ToString(), _fileStream.Length,
                                                MemoryMappedFileAccess.ReadWrite, null, HandleInheritability.None, true);
                _pagerState.Release(); // when the last transaction using this is over, will dispose it

                var accessor = mmf.CreateViewAccessor();
                byte* p = null;
                accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref p);

                var pagerState = new PagerState
                    {
                        Accessor = accessor,
                        File = mmf,
                        Base = p
                    };
                pagerState.AddRef();
                _pagerState = pagerState;
                _allocatedPages = accessor.Capacity / PageSize;
            }
            return new Page(_pagerState.Base + (n * PageSize), PageMaxSpace);
        }

        public override void Flush()
        {
            _pagerState.Accessor.Flush();
            _fileStream.Flush(true);
        }

        public override PagerState TransactionBegan()
        {
            var state = _pagerState;
            state.AddRef();
            return state;
        }

        public override void TransactionCompleted(PagerState state)
        {
            state.Release();
        }

        public override void EnsureContinious(long requestedPageNumber, int pageCount)
        {
            throw new NotImplementedException();
        }

        public override void Dispose()
        {
            if (_pagerState != null)
            {
                _pagerState.Release();
                _pagerState = null;
            }
            _fileStream.Dispose();
        }

        public override long NumberOfAllocatedPages { get { return _allocatedPages; } }
    }
}