using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow;
using Sparrow.Binary;
using Sparrow.LowMemory;
using Sparrow.Threading;
using Voron;
using Voron.Impl;

namespace Micro.Benchmark.PageLocatorImpl
{
    /// <summary>
    /// Implements a different algorithm
    /// </summary>
    public unsafe class PageLocatorCurrent
    {
        private const ushort Invalid = 0;

        private readonly ByteStringContext _allocator = new ByteStringContext();
        private readonly LowLevelTransactionStub _tx;

        [StructLayout(LayoutKind.Explicit, Size = 20)]
        private struct PageData
        {
            [FieldOffset(0)]
            public long PageNumber;
            [FieldOffset(8)]
            public MyPageStruct Page;
            [FieldOffset(16)]
            public bool IsWritable;
        }

        private PageData* _cache;
        private ByteString _cacheMemory;

        private int _andMask;

        public PageLocatorCurrent(LowLevelTransactionStub tx, int cacheSize = 8)
        {
            //Debug.Assert(tx != null);
            //Debug.Assert(cacheSize > 0);
            _tx = tx;

            if (tx != null)
                Debug.Fail("");

            cacheSize = Bits.PowerOf2(cacheSize);
            if (cacheSize > 1024)
                cacheSize = 1024;

            int shiftRight = Bits.CeilLog2(cacheSize);
            _andMask = (int)(0xFFFFFFFF >> (sizeof(uint) * 8 - shiftRight));

            _tx = tx;

            _allocator.Allocate(cacheSize * sizeof(PageData), out _cacheMemory);
            _cache = (PageData*)_cacheMemory.Ptr;

            for (var i = 0; i < cacheSize; i++)
            {
                _cache[i].PageNumber = Invalid;
            }
        }

        public MyPageStruct GetReadOnlyPage(long pageNumber)
        {
            var position = pageNumber & _andMask;

            PageData* node = &_cache[position];
            if (node->PageNumber == pageNumber)
                return node->Page;

            var page = LowLevelTransactionStub.GetPageStruct(pageNumber);
            node->PageNumber = pageNumber;
            node->Page = page;
            node->IsWritable = false;

            return page;
        }

        public MyPageStruct GetWritablePage(long pageNumber)
        {
            var position = pageNumber & _andMask;

            PageData* node = &_cache[position];

            if (node->IsWritable && node->PageNumber == pageNumber)
                return node->Page;

            var page = LowLevelTransactionStub.ModifyPageStruct(pageNumber);
            node->PageNumber = pageNumber;
            node->Page = page;
            node->IsWritable = true;

            return page;
        }

        public void Reset(long pageNumber)
        {
            var position = pageNumber & _andMask;

            if (_cache[position].PageNumber == pageNumber)
            {
                _cache[position].PageNumber = Invalid;
            }
        }
    }
}
