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

            if (!Bits.IsPowerOfTwo(cacheSize))
                cacheSize = Bits.NextPowerOf2(cacheSize);

            int shiftRight = Bits.CeilLog2(cacheSize);
            _andMask = (int) (0xFFFFFFFF >> (sizeof(uint) * 8 - shiftRight));

            _tx = tx;

            tx.Allocator.Allocate(cacheSize * sizeof(PageData), out _cacheMemory);
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
        public bool TryGetReadOnlyPage(long pageNumber, out Page page)
        {
            var position = pageNumber & _andMask;

            PageData* node = &_cache[position];
            if (node->PageNumber == pageNumber && node->PageNumber != Invalid)
            {
                page = node->Page;
                return true;
            }

            page = default(Page);
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetWritablePage(long pageNumber, out Page page)
        {
            var position = pageNumber & _andMask;

            PageData* node = &_cache[position];

            if (node->IsWritable && node->PageNumber == pageNumber && node->PageNumber != Invalid)
            {
                page = node->Page;
                return true;
            }

            page = default(Page);
            return false;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetReadable(long pageNumber, Page page)
        {
            var position = pageNumber & _andMask;

            PageData* node = &_cache[position];

            if (node->PageNumber != pageNumber)
            {
                node->PageNumber = pageNumber;
                node->Page = page;
                node->IsWritable = false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetWritable(long pageNumber, Page page)
        {
            var position = pageNumber & _andMask;

            PageData* node = &_cache[position];

            if (node->PageNumber != pageNumber || node->IsWritable == false)
            {
                node->PageNumber = pageNumber;
                node->Page = page;
                node->IsWritable = true;
            }
        }
    }
}
