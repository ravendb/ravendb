using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using Sparrow.Server;

namespace Voron.Data.RoaringBitmaps;

public unsafe partial struct RoaringBitmap
{
    /// <summary>
    /// Segregated storage-free list: one intrusive singly linked list head per size class.
    ///
    /// Storage blocks are bucketed into <see cref="NumSizeClasses"/> size classes on a 32-byte
    /// aligned x1.091 geometric ladder spanning 64 - 8,192. Allocations round their requested byte
    /// count up to the enclosing class size. All matching required <see cref="SimdAlignment"/>
    /// Worst-case internal fragmentation is the x1.091 step (~9%); measured waste is ~4%.
    /// </summary>
    private struct FreeListHeads
    {
        private const int NumSizeClasses = 42;

        private static readonly int[] ClassSize =
        [
            64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 416, 480, 544, 608, 672, 736, 832, 928,
            1024, 1120, 1248, 1376, 1504, 1664, 1824, 2016, 2208, 2432, 2656, 2912, 3200, 3520, 3872,
            4256, 4672, 5120, 5600, 6112, 6688, 7328, 8000, 8192
        ];

        /// <summary>Maps a 32-byte quantum to the smallest class whose size covers it. Indexed by <c>(neededBytes + 31) &gt;&gt; 5</c>.</summary>
        private static ReadOnlySpan<byte> ClassOfQuantum =>
        [
            0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 11, 11, 12, 12, 13, 13, 14, 14, 15, 15, 16,
            16, 16, 17, 17, 17, 18, 18, 18, 19, 19, 19, 20, 20, 20, 20, 21, 21, 21, 21, 22, 22, 22,
            22, 23, 23, 23, 23, 23, 24, 24, 24, 24, 24, 25, 25, 25, 25, 25, 25, 26, 26, 26, 26, 26,
            26, 27, 27, 27, 27, 27, 27, 27, 28, 28, 28, 28, 28, 28, 28, 29, 29, 29, 29, 29, 29, 29,
            29, 30, 30, 30, 30, 30, 30, 30, 30, 30, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 32, 32,
            32, 32, 32, 32, 32, 32, 32, 32, 32, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 34,
            34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
            35, 35, 35, 35, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 37, 37, 37,
            37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 38, 38, 38, 38, 38, 38, 38, 38, 38,
            38, 38, 38, 38, 38, 38, 38, 38, 38, 39, 39, 39, 39, 39, 39, 39, 39, 39, 39, 39, 39, 39,
            39, 39, 39, 39, 39, 39, 39, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
            40, 40, 40, 40, 40, 40, 41, 41, 41, 41, 41, 41
        ];

        [InlineArray(NumSizeClasses)]
        private struct Heads
        {
            private ByteString _head0;
        }

        private Heads _heads;

        private ulong _classMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int SizeClassFor(int bytes)
        {
            Debug.Assert(bytes is >= 0 and <= BitmapContainerSizeInBytes, "storage request out of size-class range");
            return ClassOfQuantum[(bytes + SimdAlignment - 1) >> 5];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(ByteString bs)
        {
            int c = SizeClassFor(bs.Length);
            *(ByteString*)bs.Ptr = _heads[c]; // chain: new node's next = old class head
            _heads[c] = bs;                   // new class head = bs
            _classMask |= 1UL << c;
        }

        public void Allocate(ByteStringContext ctx, int neededBytes, out ByteString storage)
        {
            int c = SizeClassFor(neededBytes);

            // we use best-fit here using single instruction with bit twiddling 
            ulong avail = _classMask & (~0UL << c);
            if (avail != 0)
            {
                int fc = BitOperations.TrailingZeroCount(avail);
                storage = _heads[fc];                  // pop head
                ByteString next = *(ByteString*)storage.Ptr;
                _heads[fc] = next;
                if (next.HasValue == false)
                    _classMask &= ~(1UL << fc);        // class now empty
                return;
            }

            ctx.Allocate(ClassSize[c], out storage);
        }

        public void ReleaseAll(ByteStringContext ctx)
        {
            ulong mask = _classMask;
            while (mask != 0)
            {
                int c = BitOperations.TrailingZeroCount(mask);
                mask &= mask - 1;

                ByteString freeNode = _heads[c];
                while (freeNode.HasValue)
                {
                    ByteString nextNode = *(ByteString*)freeNode.Ptr; // read next before releasing
                    ctx.Release(ref freeNode);
                    freeNode = nextNode;
                }
                _heads[c] = default;
            }
            _classMask = 0;
        }
    }
}
