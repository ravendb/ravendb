using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Sparrow.Server.Collections.LockFree;
using System.Text.RegularExpressions;

namespace Sparrow.Server
{
    public unsafe class AdvMemory
    {
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static int Compare(byte* p1, byte* p2, int size)
        {
            if (Avx2.IsSupported)
            {
                return CompareAvx2(p1, p2, size);
            }

            if (size <= 128)
                return CompareSmallInlineNet6OorLesser(p1, p2, size);

            return new ReadOnlySpan<byte>(p1, size).SequenceCompareTo(new ReadOnlySpan<byte>(p2, size));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(void* p1, void* p2, int size)
        {
            if (Avx2.IsSupported)
            {
                return CompareAvx2(p1, p2, size);
            }

            if (size <= 128)
                return CompareSmallInlineNet6OorLesser(p1, p2, size);

            return new ReadOnlySpan<byte>(p1, size).SequenceCompareTo(new ReadOnlySpan<byte>(p2, size));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(ref byte p1, ref byte p2, int size)
        {
            if (Avx2.IsSupported)
            {
                return CompareAvx2(ref p1, ref p2, size);
            }

            return CompareSmallInlineNet7(ref p1, ref p2, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(ReadOnlySpan<byte> p1, ReadOnlySpan<byte> p2, int size)
        {
            ref byte p1Start = ref MemoryMarshal.GetReference(p1);
            ref byte p2Start = ref MemoryMarshal.GetReference(p2);
            return CompareInline(ref p1Start, ref p2Start, size);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int CompareAvx2(void* p1, void* p2, int size)
        {
            // PERF: Given all the preparation that must happen before even accessing the pointers, even if we increase
            // the size of the method by 10+ bytes, by the time we access the data it is already there in L1 cache.
            Sse.Prefetch0(p1);
            Sse.Prefetch0(p2);

            // PERF: This allows us to do pointer arithmetic and use relative addressing using the 
            //       hardware instructions without needed an extra register.
            byte* bpx = (byte*)p1;
            nuint offset = (nuint)((byte*)p2 - bpx);

            nuint length = (nuint)size;
            byte* bpxEnd = bpx + length;

            uint matches;

            // PERF: The alignment unit will be decided in terms of the total size, because we can use the exact same code
            // for a length smaller than a vector or to force alignment to a certain memory boundary. This will cause some
            // multi-modal behavior to appear (specially close to the vector size) because we will become dependent on
            // the input. The biggest gains will be seen when the compares are several times bigger than the vector size,
            // where the aligned memory access (no penalty) will dominate the runtime. So this formula will calculate how
            // many bytes are required to get to an aligned pointer.
            nuint alignmentUnit = length >= (nuint)Vector256<byte>.Count ? (nuint)(Vector256<byte>.Count - (long)bpx % Vector256<byte>.Count) : length;
            if ((alignmentUnit & (nuint)(Vector256<byte>.Count-1)) == 0 || length is >= 32 and <= 512)
                goto ProcessAligned;

            // Check if we are completely aligned, in that case just skip everything and go straight to the
            // core of the routine. We have much bigger fishes to fry. 
            if ((alignmentUnit & 2) != 0)
            {
                if (*(ushort*)bpx != *(ushort*)(bpx + offset))
                {
                    if (*bpx == *(bpx + offset))
                        bpx++;
                    goto DoneByte;
                }

                bpx += 2;
            }

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
                goto Equals;

            // PERF: From now on, at least 1 of the two memory sites will be 4 bytes aligned. Improving the chances to
            // hit a 16 bytes (128-bits alignment) and also give us access to performed a single masked load to ensure
            // 128-bits alignment. The reason why we want that is because natural alignment can impact the L1 data cache
            // latency. 

            // For example in AMD 17th gen: A misaligned load operation suffers, at minimum, a one cycle penalty in the
            // load-store pipeline if it spans a 32-byte boundary. Throughput for misaligned loads and stores is half
            // that of aligned loads and stores since a misaligned load or store requires two cycles to access the data
            // cache (versus a single cycle for aligned loads and stores). 
            // Source: https://developer.amd.com/wordpress/media/2013/12/55723_SOG_Fam_17h_Processors_3.00.pdf

            // Now we know we are 4 bytes aligned. So now we can actually use this knowledge to perform a masked load
            // of the leftovers to achieve 32 bytes alignment. In the case that we are smaller, this will just find the
            // difference and we will jump to difference. Masked loads and stores will not cause memory access violations
            // because no memory access happens per presentation from Intel.
            // https://llvm.org/devmtg/2015-04/slides/MaskedIntrinsics.pdf

            Debug.Assert(alignmentUnit > 0, "Cannot be 0 because that means that we have completed already.");
            Debug.Assert(alignmentUnit < (nuint)Vector256<int>.Count * sizeof(int), $"Cannot be {Vector256<int>.Count * sizeof(int)} or greater because that means it is a full vector.");

            int* tablePtr = (int*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(LoadMaskTable));
            var mask = Avx.LoadDquVector256(tablePtr + ((nuint)Vector256<int>.Count - alignmentUnit / sizeof(uint)));

            matches = (uint)Avx2.MoveMask(
                Avx2.CompareEqual(
                    Avx2.MaskLoad((int*)bpx, mask).AsByte(),
                    Avx2.MaskLoad((int*)(bpx + offset), mask).AsByte()
                    )
                );

            if (matches != uint.MaxValue)
                goto Difference;

            // PERF: The reason why we don't keep the original alignment is because we want to get rid of the initial leftovers,
            // so that would require an AND instruction anyways. In this way we get the same effect using a shift. 
            bpx += alignmentUnit & unchecked((nuint)~3);

            ProcessAligned:
            byte* loopEnd = bpxEnd - (nuint)Vector256<byte>.Count;
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
                if (matches == uint.MaxValue)
                {
                    // All matched
                    bpx += (nuint)Vector256<byte>.Count;
                    continue;
                }
                goto Difference;
            }

            // If can happen that we are done so we can avoid the last unaligned access. 
            if (bpx == bpxEnd)
                goto Equals;

            bpx = loopEnd;
            matches = (uint)Avx2.MoveMask(Avx2.CompareEqual(Avx.LoadVector256(bpx), Avx.LoadVector256(bpx + offset)));
            if (matches == uint.MaxValue)
                goto Equals;

            Difference:
            // We invert matches to find differences, which are found in the bit-flag. .
            // We then add offset of first difference to the current offset in order to check that specific byte.
            bpx += (nuint)BitOperations.TrailingZeroCount(~matches);
            
            DoneByte:
            return *bpx - *(bpx + offset);

            Equals:
            return 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int CompareAvx2(ref byte p1, ref byte p2, int size)
        {
            ref byte bpx = ref p1;
            ref byte bpy = ref p2;
            ref byte bpxEnd = ref Unsafe.AddByteOffset(ref p1, size);
            if (size >= Vector256<byte>.Count)
            {
                ref byte loopEnd = ref Unsafe.SubtractByteOffset(ref bpxEnd, (nuint)Vector256<byte>.Count);

                uint matches;
                while (Unsafe.IsAddressGreaterThan(ref loopEnd, ref bpx))
                {
                    matches = (uint)Avx2.MoveMask(
                        Vector256.Equals(
                            Vector256.LoadUnsafe(ref bpx),
                            Vector256.LoadUnsafe(ref bpy)
                        )
                    );

                    // Note that MoveMask has converted the equal vector elements into a set of bit flags,
                    // So the bit position in 'matches' corresponds to the element offset.

                    // 32 elements in Vector256<byte> so we compare to uint.MaxValue to check if everything matched
                    if (matches == uint.MaxValue)
                    {
                        // All matched
                        bpx = ref Unsafe.AddByteOffset(ref bpx, (nuint)Vector256<byte>.Count);
                        bpy = ref Unsafe.AddByteOffset(ref bpy, (nuint)Vector256<byte>.Count);
                        continue;
                    }

                    goto Difference;
                }

                // If can happen that we are done so we can avoid the last unaligned access. 
                if (Unsafe.AreSame(ref bpx, ref bpxEnd))
                    return 0;

                bpx = ref loopEnd;
                bpy = ref Unsafe.AddByteOffset(ref p2, size - Vector256<byte>.Count);
                matches = (uint)Avx2.MoveMask(
                    Vector256.Equals(
                        Vector256.LoadUnsafe(ref bpx),
                        Vector256.LoadUnsafe(ref bpy)
                    )
                );

                if (matches == uint.MaxValue)
                    return 0;

                Difference:
                // We invert matches to find differences, which are found in the bit-flag. .
                // We then add offset of first difference to the current offset in order to check that specific byte.
                var bytesToAdvance = (nuint)BitOperations.TrailingZeroCount(~matches);
                bpx = ref Unsafe.AddByteOffset(ref bpx, bytesToAdvance);
                bpy = ref Unsafe.AddByteOffset(ref bpy, bytesToAdvance);
                return bpx - bpy;
            }

            return CompareSmallInlineNet7(ref p1, ref p2, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        internal static int CompareSmallInlineNet6OorLesser(void* p1, void* p2, int size)
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
        internal static int CompareSmallInlineNet7(ref byte p1, ref byte p2, int size)
        {
            ref byte bpx = ref Unsafe.AddByteOffset(ref p1, size);
            ref byte bpy = ref Unsafe.AddByteOffset(ref p2, size);

            // PERF: This allows us to do pointer arithmetics and use relative addressing using the 
            //       hardware instructions without needed an extra register.            

            if (size < sizeof(ulong))
            {
                // We know the size is not big enough to be able to do what we intend to do. Therefore,
                // we will use a very compact code that will be easily inlined and don't bloat the caller site.
                // If size if big enough, we could easily pay for the actual call to compare as the cost of
                // the method call can get diluted.
                while (size > 0)
                {
                    byte vx = Unsafe.SubtractByteOffset(ref bpx, size);
                    byte vy = Unsafe.SubtractByteOffset(ref bpy, size);
                    // We check the values.
                    if (vx == vy)
                    {
                        size -= 1;
                        continue;
                    }

                    return vx - vy;
                }

                return 0;
            }

            // We now know that we have enough space to actually do what we want. 
            ulong rbpx = 0;
            ulong rbpy = 0;
            while (size >= sizeof(ulong))
            {
                rbpx = Unsafe.ReadUnaligned<ulong>(ref Unsafe.SubtractByteOffset(ref bpx, size));
                rbpy = Unsafe.ReadUnaligned<ulong>(ref Unsafe.SubtractByteOffset(ref bpy, size));

                // PERF: JIT will emit: ```{op} {reg}, qword ptr [rdx+rax]```
                if (rbpx == rbpy)
                {
                    size -= sizeof(ulong);
                    continue;
                }

                goto DONE;
            }

            if (size > 0)
            {
                size = sizeof(ulong);
                rbpx = Unsafe.ReadUnaligned<ulong>(ref Unsafe.SubtractByteOffset(ref bpx, size));
                rbpy = Unsafe.ReadUnaligned<ulong>(ref Unsafe.SubtractByteOffset(ref bpy, size));
            }

            DONE:
            ulong xor = rbpx ^ rbpy;

            // Correctness path for equals. IF the XOR is actually zero, we are done. 
            if (xor == 0)
                return 0;

            // PERF: This is a bit twiddling hack. Given that bitwise xor-ing 2 values flag the bits difference, 
            //       we can use that we know we are running on little endian hardware and the very first bit set 
            //       will correspond to the first byte which is different. 
            size -= BitOperations.TrailingZeroCount(xor) / 8;

            return Unsafe.SubtractByteOffset(ref bpx, size) - Unsafe.SubtractByteOffset(ref bpy, size);
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

                    return *bpx - *(bpx + offset);
                }

                return 0;
            }

            // We now know that we have enough space to actually do what we want. 
            ulong xor;

            byte* loopEnd = bpxEnd - sizeof(ulong);
            while (bpx <= loopEnd)
            {
                // PERF: JIT will emit: ```{op} {reg}, qword ptr [rdx+rax]```
                xor = *(ulong*)bpx ^ *(ulong*)(bpx + offset);
                if (xor != 0)
                    goto DONE;

                bpx += 8;
            }

            bpx = loopEnd;
            xor = *(ulong*)bpx ^ *(ulong*)(bpx + offset);
            if (xor == 0) // Correctness path for equals. IF the XOR is actually zero, we are done. 
                return 0;
            
            DONE:
            // PERF: This is a bit twiddling hack. Given that bitwise xoring 2 values flag the bits difference, 
            //       we can use that we know we are running on little endian hardware and the very first bit set 
            //       will correspond to the first byte which is different. 
            bpx += (long)BitOperations.TrailingZeroCount(xor) / 8;
            return *bpx - *(bpx + offset);
        }
    }
}
