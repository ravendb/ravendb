using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Binary;

namespace Sparrow.Server
{
    public unsafe class AdvMemory
    {
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static int Compare(byte* p1, byte* p2, int size)
        {
            if (size <= 128)
                return CompareSmallInlineNet6OrLesser(p1, p2, size);
            
            return new ReadOnlySpan<byte>(p1, size).SequenceCompareTo(new ReadOnlySpan<byte>(p2, size));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareSmall(void* p1, void* p2, int size)
        {
            return CompareAvx2(p1, p2, size);
        }

        private static ReadOnlySpan<byte> LoadMaskTable => new byte[]
        {
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        internal static int CompareAvx2(void* p1, void* p2, int size)
        {
            byte* bpx = (byte*)p1;

            // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
            //       hardware instructions without needed an extra register.            
            long offset = (byte*)p2 - bpx;

            // Check if we are completely aligned, in that case just skip everything and go straight to the
            // core of the routine. We have much bigger fishes to fry. 
            byte* loopEnd = bpx + (size & 3);
            while (bpx < loopEnd)
            {
                if (*bpx != *(bpx + offset))
                    goto Tail;

                bpx++;
            }

            byte* bpxEnd = (byte*)p1 + size;
            if (bpx == bpxEnd)
                return 0;

            uint matches = uint.MaxValue;

            // Now we know we are 32 bits aligned. So now we can actually use this knowledge to perform a masked load
            // of the leftovers needed to align. In the case that we are smaller, this will just find the difference
            // and we will jump to difference. Essentially we can have up-to 31 integers to load. 
            // Masked loads and stores will not cause memory access violations because no memory access happens per presentation from Intel.
            // https://llvm.org/devmtg/2015-04/slides/MaskedIntrinsics.pdf
            int length = (int)(bpxEnd - bpx);
            int alignmentUnit = length & (Vector256<byte>.Count - 1);
            if (alignmentUnit == 0)
                goto ProcessAligned;

            Debug.Assert(alignmentUnit / sizeof(int) != 0, "Cannot be 0 because that means it is aligned.");
            Debug.Assert(alignmentUnit / sizeof(int) < Vector256<int>.Count, $"Cannot be {Vector256<int>.Count} or greater because that means it is aligned.");

            int* tablePtr = (int*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(LoadMaskTable));
            var mask = Avx2.LoadDquVector256(tablePtr + (Vector256<int>.Count - alignmentUnit / sizeof(uint)));

            matches = (uint)Avx2.MoveMask(
                Avx2.CompareEqual(
                    Avx2.MaskLoad((int*)bpx, mask).AsByte(),
                    Avx2.MaskLoad((int*)(bpx + offset), mask).AsByte()
                    )
                );

            if (matches != uint.MaxValue)
                goto Difference;

            bpx += alignmentUnit;
            if (bpx == bpxEnd)
                return 0;

            ProcessAligned:
            loopEnd = bpxEnd - Vector256<byte>.Count;
            while (bpx <= loopEnd)
            {
                matches = (uint)Avx2.MoveMask(Avx2.CompareEqual(Avx.LoadVector256(bpx), Avx.LoadVector256(bpx + offset)));

                // Note that MoveMask has converted the equal vector elements into a set of bit flags,
                // So the bit position in 'matches' corresponds to the element offset.

                // 32 elements in Vector256<byte> so we compare to uint.MaxValue to check if everything matched
                if (matches == uint.MaxValue)
                {
                    // All matched
                    bpx += (nuint)Vector256<byte>.Count;
                    continue;
                }

                goto Difference;
            }

            if (bpx != bpxEnd)
            {
                bpx = bpxEnd - Vector256<byte>.Count;
                matches = (uint)Avx2.MoveMask(Avx2.CompareEqual(Avx.LoadVector256(bpx), Avx.LoadVector256(bpx + offset)));
            }

            if (matches == uint.MaxValue)
            {
                // All matched
                return 0;
            }

            Difference:
            // Invert matches to find differences
            nuint differences = ~matches;

            // Find bitflag offset of first difference and add to current offset
            bpx += (uint)BitOperations.TrailingZeroCount(differences);

            Tail:
            return *bpx - *(bpx + offset);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        internal static int CompareAvx2Expanded(void* p1, void* p2, int size)
        {
            byte* bpx = (byte*)p1;
            byte* bpy = (byte*)p2;

            nuint length = (nuint) size;

            // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
            //       hardware instructions without needed an extra register.            
            long offset = bpy - bpx;
            byte* bpxEnd = bpx + length;

            uint matches = uint.MaxValue;

            // Check if we are completely aligned, in that case just skip everything and go straight to the
            // core of the routine. We have much bigger fishes to fry. 
            nuint alignmentUnit = length & (nuint)(Vector256<byte>.Count - 1);
            if (alignmentUnit == 0)
                goto ProcessAligned;

            if ((alignmentUnit & 2) != 0)
            {
                if (*(ushort*)bpx != *(ushort*)(bpx + offset))
                    goto DoneShort;

                bpx += 2;
            }

            int result = 0;

            // We have a potential problem. As AVX2 doesn't provide us a masked load that could address bytes
            // we will need to ensure we are int aligned. Therefore, we have to do this as fast as possibly. 
            if ((alignmentUnit & 1) != 0)
            {
                if (*bpx != *(bpx + offset))
                    goto DoneByte;

                bpx += 1;
            }

            length = (nuint)(bpxEnd - bpx);
            if (length == 0)
                goto DoneByte;

            goto EnsureAlignedBlock;

            DoneShort:
            bpx += (*bpx == *(bpx + offset)).ToInt32();
            DoneByte:
            return *bpx - *(bpx + offset);

            EnsureAlignedBlock:

            // Now we know we are 32 bits aligned. So now we can actually use this knowledge to perform a masked load
            // of the leftovers needed to align. In the case that we are smaller, this will just find the difference
            // and we will jump to difference. Essentially we can have up-to 31 integers to load. 
            // Masked loads and stores will not cause memory access violations because no memory access happens per presentation from Intel.
            // https://llvm.org/devmtg/2015-04/slides/MaskedIntrinsics.pdf
            alignmentUnit = length & (nuint)(Vector256<byte>.Count - 1);
            if (alignmentUnit == 0)
                goto ProcessAligned;

            Debug.Assert(alignmentUnit / sizeof(int) != 0, "Cannot be 0 because that means it is aligned.");
            Debug.Assert(alignmentUnit / sizeof(int) < (nuint)Vector256<int>.Count, $"Cannot be {Vector256<int>.Count} or greater because that means it is aligned.");

            int* tablePtr = (int*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(LoadMaskTable));
            var mask = Avx2.LoadDquVector256(tablePtr + ((nuint)Vector256<int>.Count - alignmentUnit / sizeof(uint)));

            matches = (uint)Avx2.MoveMask(
                Avx2.CompareEqual(
                    Avx2.MaskLoad((int*)bpx, mask).AsByte(),
                    Avx2.MaskLoad((int*)(bpx + offset), mask).AsByte()
                    )
                );

            if (matches != uint.MaxValue)
                goto Difference;

            bpx += alignmentUnit;
            if (bpx == bpxEnd)
                return 0;

            ProcessAligned:
            byte* loopEnd = bpxEnd - Vector256<byte>.Count;
            while (bpx <= loopEnd)
            {
                matches = (uint)Avx2.MoveMask(
                                    Avx2.CompareEqual(
                                        Avx.LoadVector256(bpx), 
                                        Avx.LoadVector256(bpx + offset)
                                        )
                                    );

                // Note that MoveMask has converted the equal vector elements into a set of bit flags,
                // So the bit position in 'matches' corresponds to the element offset.

                // 32 elements in Vector256<byte> so we compare to uint.MaxValue to check if everything matched
                if (matches != uint.MaxValue)
                    goto Difference;
    
                // All matched
                bpx += (nuint)Vector256<byte>.Count;
            }

            if (bpx != bpxEnd)
            {
                bpx = bpxEnd - Vector256<byte>.Count;
                matches = (uint)Avx2.MoveMask(Avx2.CompareEqual(Avx.LoadVector256(bpx), Avx.LoadVector256(bpx + offset)));
            }

            if (matches == uint.MaxValue)
            {
                // All matched
                return 0;
            }

            Difference:

            // Invert matches to find differences
            nuint differences = ~matches;

            // Find bitflag offset of first difference and add to current offset
            bpx += (uint)BitOperations.TrailingZeroCount(differences);

            result = *bpx - *(bpx + offset);
            Debug.Assert(result != 0);
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        internal static int CompareAvx2Naive(void* p1, void* p2, int size)
        {
            byte* bpx = (byte*)p1;
            byte* bpy = (byte*)p2;

            // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
            //       hardware instructions without needed an extra register.            
            long offset = bpy - bpx;
            byte* bpxEnd = bpx + size;


            if (size >= Vector256<byte>.Count)
            {
                uint matches = 0;

                byte* loopEnd = bpxEnd - Vector256<byte>.Count;
                while (bpx <= loopEnd)
                {
                    matches = (uint)Avx2.MoveMask(Avx2.CompareEqual(Avx.LoadVector256(bpx), Avx.LoadVector256(bpx + offset)));

                    // Note that MoveMask has converted the equal vector elements into a set of bit flags,
                    // So the bit position in 'matches' corresponds to the element offset.

                    // 32 elements in Vector256<byte> so we compare to uint.MaxValue to check if everything matched
                    if (matches == uint.MaxValue)
                    {
                        // All matched
                        bpx += (nuint)Vector256<byte>.Count;
                        continue;
                    }

                    goto Difference;
                }

                if (bpx != bpxEnd)
                {
                    bpx = bpxEnd - Vector256<byte>.Count;
                    matches = (uint)Avx2.MoveMask(Avx2.CompareEqual(Avx.LoadVector256(bpx), Avx.LoadVector256(bpx + offset)));
                }

                if (matches == uint.MaxValue)
                {
                    // All matched
                    return 0;
                }

            Difference:

                // Invert matches to find differences
                uint differences = ~matches;

                // Find bitflag offset of first difference and add to current offset
                bpx += (uint)BitOperations.TrailingZeroCount(differences);

                int result = *bpx - *(bpx + offset);
                Debug.Assert(result != 0);
                return result;
            }

            // We know the size is not big enough to be able to do what we intend to do. Therefore,
            // we will use a very compact code that will be easily inlined and don't bloat the caller site.
            // If size if big enough, we could easily pay for the actual call to compare as the cost of
            // the method call can get diluted.
            while (bpx < bpxEnd)
            {
                if (*bpx == *(bpx + offset))
                {
                    bpx++;
                    continue;
                }

                return *bpx - *(bpx + offset);
            }

            return 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        internal static int CompareSmallInlineNet6OrLesser(void* p1, void* p2, int size)
        {
            byte* bpx = (byte*)p1;
            byte* bpy = (byte*)p2;

            // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
            //       hardware instructions without needed an extra register.            
            long offset = bpy - bpx;
            byte* bpxEnd = bpx + size;

            if (size < sizeof(ulong))
            {
                // We know the size is not big enough to be able to do what we intend to do. Therefore,
                // we will use a very compact code that will be easily inlined and don't bloat the caller site.
                // If size is big enough, we could easily pay for the actual call to compare as the cost of
                // the method call can get diluted.

                long uix = 0;
                long uiy = 0;
                if ((size & 4) != 0)
                {
                    uix = *(uint*)bpx;
                    uiy = *(uint*)(bpx + offset);

                    if ((uix ^ uiy) != 0)
                    {
                        uix <<= 32;
                        uiy <<= 32;
                        goto SMALL_TAIL;
                    }

                    bpx += 4;
                }

                if ((size & 2) != 0)
                {
                    uix = *(ushort*)bpx;
                    uiy = *(ushort*)(bpx + offset);
                    if ((uix ^ uiy) != 0)
                    {
                        uix <<= 32 + 16;
                        uiy <<= 32 + 16;
                        goto SMALL_TAIL;
                    }
                    bpx += 2;
                }

                if ((size & 1) != 0)
                {
                    return *bpx - *(bpx + offset);
                }

            SMALL_TAIL:
                return Math.Sign(BinaryPrimitives.ReverseEndianness(uix) - BinaryPrimitives.ReverseEndianness(uiy));
            }

            // We now know that we have enough space to actually do what we want. 
            ulong xor = 0;

            byte* loopEnd = bpxEnd - sizeof(ulong);
            while (bpx <= loopEnd)
            {
                // PERF: JIT will emit: ```{op} {reg}, qword ptr [rdx+rax]```
                xor = *(ulong*)bpx ^ *(ulong*)(bpx + offset);
                if (xor != 0)
                    goto TAIL;

                bpx += 8;
            }

            if (bpx != bpxEnd)
            {
                bpx = bpxEnd - sizeof(ulong);
                xor = *(ulong*)bpx ^ *(ulong*)(bpx + offset);
            }

        TAIL:
            // Correctness path for equals. IF the XOR is actually zero, we are done. 
            if (xor == 0)
                return 0;

            // PERF: This is a bit twiddling hack. Given that bitwise xoring 2 values flag the bits difference, 
            //       we can use that we know we are running on little endian hardware and the very first bit set 
            //       will correspond to the first byte which is different. 
            bpx += (long)BitOperations.TrailingZeroCount(xor) / 8;
            return *bpx - *(bpx + offset);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int CompareSmallInlineNet7(void* p1, void* p2, int size)
        {
            byte* bpx = (byte*)p1;
            byte* bpy = (byte*)p2;

            // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
            //       hardware instructions without needed an extra register.            
            long offset = bpy - bpx;
            byte* bpxEnd = bpx + size;

            long result = 0;
            if (size < sizeof(ulong))
            {
                // We know the size is not big enough to be able to do what we intend to do. Therefore,
                // we will use a very compact code that will be easily inlined and don't bloat the caller site.
                // If size if big enough, we could easily pay for the actual call to compare as the cost of
                // the method call can get diluted.
                while (bpx < bpxEnd)
                {
                    if (*bpx == *(bpx + offset))
                    {
                        bpx++;
                        continue;
                    }

                    result = *bpx - *(bpx + offset);
                    goto DONE;
                }

                goto DONE;
            }

            // We now know that we have enough space to actually do what we want. 
            while (bpx < bpxEnd)
            {
                // PERF: JIT will emit: ```{op} {reg}, qword ptr [rdx+rax]```
                if (*(ulong*)bpx == *(ulong*)(bpx + offset))
                {
                    bpx += sizeof(long);
                    continue;
                }

                result = BinaryPrimitives.ReverseEndianness(*(long*)bpx) - BinaryPrimitives.ReverseEndianness(*(long*)(bpx + offset));
                goto DONE;
            }

            if (bpx != bpxEnd)
            {
                long vx = BinaryPrimitives.ReverseEndianness(*(long*)(bpxEnd - sizeof(ulong)));
                long vy = BinaryPrimitives.ReverseEndianness(*(long*)(bpxEnd - sizeof(ulong) + offset));
                result = vx - vy;
            }

            DONE:
            return Math.Sign(result);
        }
    }
}
