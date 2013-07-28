using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Nevar.Trees;

namespace Nevar.Impl
{
    public unsafe class PureMemoryPager : IVirtualPager
    {
        private readonly List<MemoryHandle> _handles = new List<MemoryHandle>();

        private int SegmentSize
        {
            get { return PageSize * 1024; }
        }

        public PureMemoryPager()
        {
            MaxNodeSize = (PageSize - Constants.PageHeaderSize) / Constants.MinKeysInPage;
            PageMaxSpace = PageSize - Constants.PageHeaderSize;
            PageMinSpace = (int)(PageMaxSpace * 0.33);
        }

        public int PageMaxSpace { get; private set; }
        public int MaxNodeSize { get; private set; }
        public int PageMinSpace { get; private set; }

        public void Dispose()
        {
            foreach (MemoryHandle memoryHandle in _handles)
            {
                Marshal.FreeHGlobal(memoryHandle.Ptr);
            }
            _handles.Clear();
        }

        public Page Get(long n)
        {
            long index = n / SegmentSize;
            if (index == _handles.Count)
            {
                AddSegment();
            }
            byte* pageStart = _handles[(int)index].Base + (n % SegmentSize * PageSize);
            return new Page(pageStart, PageSize);
        }

        public long NumberOfAllocatedPages
        {
            get { return _handles.Count * (SegmentSize / PageSize); }
        }

        public int PageSize
        {
            get { return 4096; }
        }

        public void Flush()
        {
        }

        private void AddSegment()
        {
            IntPtr ptr = Marshal.AllocHGlobal(SegmentSize);
            _handles.Add(new MemoryHandle
                {
                    Base = (byte*)ptr.ToPointer(),
                    Ptr = ptr
                });
        }

        public class MemoryHandle
        {
            public byte* Base;
            public IntPtr Ptr;
        }
    }
}