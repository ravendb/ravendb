using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Nevar.Trees;

namespace Nevar.Impl
{
    public unsafe class PureMemoryPager : AbstractPager
    {
        private readonly List<MemoryHandle> _handles = new List<MemoryHandle>();

        private int SegmentSize
        {
            get { return PageSize * 1024; }
        }

        public override long NumberOfAllocatedPages
        {
            get { return _handles.Count * (SegmentSize / PageSize); }
        }

        public override void Dispose()
        {
            foreach (MemoryHandle memoryHandle in _handles)
            {
                Marshal.FreeHGlobal(memoryHandle.Ptr);
            }
            _handles.Clear();
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
            long index = n / SegmentSize;
            if (index == _handles.Count)
            {
                AddSegment();
            }
            byte* pageStart = _handles[(int)index].Base + (n % SegmentSize * PageSize);
            return new Page(pageStart, PageSize);
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