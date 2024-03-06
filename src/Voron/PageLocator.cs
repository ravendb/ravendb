using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Voron
{

    public sealed class PageLocator
    {
        [StructLayout(LayoutKind.Explicit, Size = 20)]
        private struct PageData
        {
            [FieldOffset(0)]
            public long PageNumber;
            [FieldOffset(8)]
            public Page Page;
            [FieldOffset(16)]
            public ushort Generation;
            [FieldOffset(18)]
            public bool IsWritable;
        }

        private const long Invalid = -1;
        private ushort _generation = 1;

        private readonly PageData[] _cache = new PageData[CacheSize];

        private const uint CacheSize = 1024;
        private const uint CacheMask = CacheSize - 1;


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetReadOnlyPage(long pageNumber, out Page page)
        {
            ulong bucket = (ulong)pageNumber & CacheMask;
            Debug.Assert(bucket is >= 0 and < CacheSize);

            ref var node = ref Unsafe.Add(ref _cache[0], (int)bucket);
            page = node.Page;
            return node.Generation == _generation && node.PageNumber == pageNumber && node.PageNumber != Invalid;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetWritablePage(long pageNumber, out Page page)
        {
            ulong bucket = (ulong)pageNumber & CacheMask;
            Debug.Assert(bucket is >= 0 and < CacheSize);

            ref var node = ref Unsafe.Add(ref _cache[0], (int)bucket);
            page = node.Page;
            return node.Generation == _generation && node.IsWritable && node.PageNumber == pageNumber && node.PageNumber != Invalid;
        }

        public void Renew()
        {
            _generation++;

            // We are using 16 bits to store the generation, therefore if we are reusing the values after an overflow,
            // we should reset the whole array. 
            if (_generation == 0)
            {
                _generation = 1;
                MemoryMarshal.Cast<PageData, byte>(_cache).Fill(0);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset(long pageNumber)
        {
            ulong bucket = (ulong)pageNumber & CacheMask;
            Debug.Assert(bucket is >= 0 and < CacheSize);

            ref var node = ref Unsafe.Add(ref _cache[0], (int)bucket);

            if (node.PageNumber == pageNumber)
            {
                node.PageNumber = Invalid;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetReadable(Page page)
        {
            long pageNumber = page.PageNumber;

            ulong bucket = (ulong)pageNumber & CacheMask;
            Debug.Assert(bucket is >= 0 and < CacheSize);

            ref var node = ref Unsafe.Add(ref _cache[0], (int)bucket);

            if (node.PageNumber != pageNumber)
            {
                node.PageNumber = pageNumber;
                node.Page = page;
                node.Generation = _generation;
                node.IsWritable = false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetWritable(Page page)
        {
            long pageNumber = page.PageNumber;

            ulong bucket = (ulong)pageNumber & CacheMask;
            Debug.Assert(bucket is >= 0 and < CacheSize);

            ref var node = ref Unsafe.Add(ref _cache[0], (int)bucket);

            if (node.PageNumber != pageNumber || node.IsWritable == false)
            {
                node.PageNumber = pageNumber;
                node.Page = page;
                node.Generation = _generation;
                node.IsWritable = true;
            }
        }
    }
}
