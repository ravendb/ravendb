using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Nevar.Trees;

namespace Nevar.Impl
{
    public unsafe class PureMemoryPager : AbstractPager
    {
        private IntPtr _ptr;
        private long _allocatedSize;
        private byte* _base;
        private long _allocatedPages;
        private PagerState _pagerState;

        public PureMemoryPager()
        {
            _ptr = Marshal.AllocHGlobal(MinIncreaseSize);
            _base = (byte*)_ptr.ToPointer();
            _allocatedPages = _allocatedSize / PageSize;
            _pagerState = new PagerState
                {
                    Ptr = _ptr
                };
            _pagerState.AddRef();
        }


        public override long NumberOfAllocatedPages
        {
            get { return _allocatedPages; }
        }

        public override void EnsureContinious(long requestedPageNumber, int pageCount)
        {
            for (int i = 0; i < pageCount; i++)
            {
                EnsurePageExists(requestedPageNumber + i);
            }
        }

        public override void Dispose()
        {
            _pagerState.Release();
            _base = null;
        }

        public override void Flush()
        {
        }

        public override PagerState TransactionBegan()
        {
            return null;
        }

        public override void TransactionCompleted(PagerState state)
        {
        }

        public override Page Get(long n)
        {
            EnsurePageExists(n);
            return new Page(_base + (n * PageSize), PageMaxSpace);
        }

        private void EnsurePageExists(long n)
        {
            if (n >= _allocatedPages)
            {
                var oldSize = _allocatedSize;
                _allocatedSize = GetNewLength(_allocatedSize);
                _allocatedPages = _allocatedSize/PageSize;
                var newPtr = Marshal.AllocHGlobal(new IntPtr(_allocatedSize));
                var newBase = (byte*) newPtr.ToPointer();
                NativeMethods.memcpy(newBase, _base, new IntPtr(oldSize));
                _base = newBase;
                _ptr = newPtr;

                var oldPager = _pagerState;

                var newPager = new PagerState {Ptr = newPtr};
                newPager.AddRef();
                _pagerState = newPager;
                oldPager.Release();
            }
        }
    }
}