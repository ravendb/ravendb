using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Binary;
using Voron.Impl;

namespace Voron
{

    public unsafe class PageLocator
    {
        [StructLayout(LayoutKind.Explicit, Size = 20)]
        private struct PageData
        {
            [FieldOffset(0)]
            public long PageNumber;
            [FieldOffset(8)]
            public Page Page;
            [FieldOffset(16)]
            public bool IsWritable;
        }

        private const long Invalid = -1;
        private LowLevelTransaction _tx;

        private PageData* _cache;
        private ByteString _cacheMemory;

        private int _andMask;

        public int PageSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _tx.PageSize; }
        }

        public void Release()
        {
            if (_tx == null)
                return;

            _tx.Allocator.Release(ref _cacheMemory);
            _tx = null;
            _cache = null;
        }

        public void Renew(LowLevelTransaction tx, int cacheSize)
        {
            Debug.Assert(tx != null);
            Debug.Assert(cacheSize > 0);
            Debug.Assert(cacheSize <= 1024);

            cacheSize = Bits.NextPowerOf2(cacheSize);
            if (cacheSize > 1024)
                cacheSize = 1024;

            int shiftRight = Bits.CeilLog2(cacheSize);
            _andMask = (int) (0xFFFFFFFF >> (sizeof(uint) * 8 - shiftRight));

            _tx = tx;

            _cacheMemory = tx.Allocator.Allocate(cacheSize * sizeof(PageData));
            _cache = (PageData*)_cacheMemory.Ptr;

            for (var i = 0; i < cacheSize; i++)
            {
                _cache[i].PageNumber = Invalid;
            }
        }

        public PageLocator(LowLevelTransaction tx, int cacheSize = 8)
        {
            Renew(tx, cacheSize);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Page GetReadOnlyPage(long pageNumber)
        {
            var position = pageNumber & _andMask;

            PageData* node = &_cache[position];
            if (node->PageNumber == pageNumber)
                return node->Page;

            var page = _tx.GetPage(pageNumber);
            node->PageNumber = pageNumber;
            node->Page = page;
            node->IsWritable = false;

            return page;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Page GetWritablePage(long pageNumber)
        {
            var position = pageNumber & _andMask;

            PageData* node = &_cache[position];

            if (node->IsWritable && node->PageNumber == pageNumber)
                return node->Page;

            var page = _tx.ModifyPage(pageNumber);
            node->PageNumber = pageNumber;
            node->Page = page;
            node->IsWritable = true;

            return page;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
