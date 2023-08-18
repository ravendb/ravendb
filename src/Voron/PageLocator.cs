using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Binary;
using Sparrow.Server;
using Voron.Impl;

namespace Voron
{

    public sealed unsafe class PageLocator
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

        private const uint CacheSize = 1024;
        private const uint CacheMask = CacheSize - 1;

        public void Release()
        {
            if (_tx == null)
                return;

            _tx.Allocator.Release(ref _cacheMemory);
            _tx = null;
            _cache = null;
        }

        public void Renew(LowLevelTransaction tx)
        {
            Debug.Assert(tx != null);

            _tx = tx;

            tx.Allocator.Allocate((int)CacheSize * sizeof(PageData), out _cacheMemory);
            _cache = (PageData*)_cacheMemory.Ptr;

            for (var i = 0; i < CacheSize; i++)
            {
                _cache[i].PageNumber = Invalid;
            }
        }

        public PageLocator(LowLevelTransaction tx)
        {
            Renew(tx);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetReadOnlyPage(long pageNumber, out Page page)
        {
            Debug.Assert(pageNumber != Invalid);

            ref var node = ref _cache[pageNumber & CacheMask];

            if (node.PageNumber == pageNumber && node.PageNumber != Invalid)
            {
                page = node.Page;
                return true;
            }

            page = default(Page);
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetWritablePage(long pageNumber, out Page page)
        {
            Debug.Assert(pageNumber != Invalid);

            ref var node = ref _cache[pageNumber & CacheMask];

            if (node.IsWritable && node.PageNumber == pageNumber && node.PageNumber != Invalid)
            {
                page = node.Page;
                return true;
            }

            page = default(Page);
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset(long pageNumber)
        {
            ref var node = ref _cache[pageNumber & CacheMask];

            if (node.PageNumber == pageNumber)
            {
                node.PageNumber = Invalid;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetReadable(long pageNumber, Page page)
        {
            ref var node = ref _cache[pageNumber & CacheMask];

            if (node.PageNumber != pageNumber)
            {
                node.PageNumber = pageNumber;
                node.Page = page;
                node.IsWritable = false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetWritable(long pageNumber, Page page)
        {
            ref var node = ref _cache[pageNumber & CacheMask];

            if (node.PageNumber != pageNumber || node.IsWritable == false)
            {
                node.PageNumber = pageNumber;
                node.Page = page;
                node.IsWritable = true;
            }
        }
    }
}
