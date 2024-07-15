using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security;
using Sparrow.Binary;

#if NET7_0_OR_GREATER
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
#endif 

namespace Sparrow
{
    public static unsafe class Memory
    {
#if NET7_0_OR_GREATER
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
        internal static int CompareAvx256(void* p1, void* p2, int size)
        {
            Debug.Assert(AdvInstructionSet.X86.IsSupportedAvx256);
            
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

            int N = Vector256<byte>.Count;

            // PERF: The alignment unit will be decided in terms of the total size, because we can use the exact same code
            // for a length smaller than a vector or to force alignment to a certain memory boundary. This will cause some
            // multi-modal behavior to appear (specially close to the vector size) because we will become dependent on
            // the input. The biggest gains will be seen when the compares are several times bigger than the vector size,
            // where the aligned memory access (no penalty) will dominate the runtime. So this formula will calculate how
            // many bytes are required to get to an aligned pointer.
            nuint alignmentUnit = length >= (nuint)N ? (nuint)(N - (long)bpx % N) : length;
            if ((alignmentUnit & (nuint)(N - 1)) == 0 || length is >= 32 and <= 512)
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
            // hit 16 bytes (128-bits alignment) and also give us access to perform a single masked load to ensure
            // 128-bits alignment. The reason why we want that is because natural alignment can impact the L1 data cache
            // latency. 

            // For example in AMD 17th gen: A misaligned load operation suffers, at minimum, a one cycle penalty in the
            // load-store pipeline if it spans a 32-byte boundary. Throughput for misaligned loads and stores is half
            // that of aligned loads and stores since a misaligned load or store requires two cycles to access the data
            // cache (versus a single cycle for aligned loads and stores). 
            // Source: https://developer.amd.com/wordpress/media/2013/12/55723_SOG_Fam_17h_Processors_3.00.pdf

            // Now we know we are 4 bytes aligned. So now we can actually use this knowledge to perform a masked load
            // of the leftovers to achieve 32 bytes alignment. In the case that we are smaller, this will just find the
            // difference, and we will jump to difference. Masked loads and stores will not cause memory access violations
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
            byte* loopEnd = bpxEnd - (nuint)N;
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
                    bpx += (nuint)N;
                    continue;
                }
                goto Difference;
            }

            // If it can happen that we are done, we can avoid the last unaligned access. 
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
#if NET8_0_OR_GREATER                
        internal static int CompareAvx256(scoped ref readonly byte p1, scoped ref readonly byte p2, int size)
#elif NET7_0_OR_GREATER
        internal static int CompareAvx256(ref byte p1, ref byte p2, int size)
#endif
        {
            Debug.Assert(AdvInstructionSet.X86.IsSupportedAvx256);
            
            ref byte bpx = ref Unsafe.AsRef(in p1);
            ref byte bpy = ref Unsafe.AsRef(in p2);
            ref byte bpxEnd = ref Unsafe.AddByteOffset(ref bpx, size);
            
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
                bpy = ref Unsafe.AddByteOffset(ref Unsafe.AsRef(in p2), size - Vector256<byte>.Count);
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

            return CompareSmallInlineNet7(in p1, in p2, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int CompareSmallInlineNet7(scoped ref readonly byte p1, scoped ref readonly byte p2, int size)
        {
            ref byte bpx = ref Unsafe.AddByteOffset(ref Unsafe.AsRef(in p1), size);
            ref byte bpy = ref Unsafe.AddByteOffset(ref Unsafe.AsRef(in p2), size);

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
        internal static bool IsEqualConstantAvx256(ref byte constantRef, byte* ptr, int size)
        {
            Debug.Assert(AdvInstructionSet.X86.IsSupportedAvx256);

            if (size >= Vector256<byte>.Count)
            {
                Vector256<byte> result = Vector256.Equals(
                    Vector256.LoadUnsafe(ref constantRef),
                    Vector256.Load(ptr));

                if (!Vector256.EqualsAll(result, Vector256<byte>.AllBitsSet))
                    return false;

                constantRef = ref Unsafe.AddByteOffset(ref constantRef, Vector256<byte>.Count);
                ptr += Vector256<byte>.Count;
            }

            // If we have a small value and we cannot do this using just a MoveMask,
            // we will go and do it in the old fashion way (scalar).
            if (size % sizeof(uint) != 0 && size % Vector256<byte>.Count < 16)
                goto Scalar;

            // Now we know we are 4 bytes aligned or it was big enough for it to make sense.
            // So now we can actually use this knowledge to perform a masked load of the leftovers to achieve 32 bytes alignment.
            // In the case that we are smaller, this will just find the difference and we will jump to difference.
            // Masked loads and stores will not cause memory access violations because no memory access happens per presentation from Intel.
            // https://llvm.org/devmtg/2015-04/slides/MaskedIntrinsics.pdf
            int* tablePtr = (int*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(LoadMaskTable));
            var mask = Avx.LoadDquVector256(tablePtr + ((nuint)Vector256<int>.Count - (nuint)(size % Vector256<byte>.Count / sizeof(uint))));

            uint matches = (uint)Avx2.MoveMask(
                Avx2.CompareEqual(
                    Avx2.MaskLoad((int*)Unsafe.AsPointer(ref constantRef), mask).AsByte(),
                    Avx2.MaskLoad((int*)ptr, mask).AsByte()
                )
            );

            if (size % sizeof(uint) == 0)
                return matches == uint.MaxValue;

            constantRef = ref Unsafe.AddByteOffset(ref constantRef, (size % Vector256<byte>.Count / sizeof(uint)) * sizeof(uint));
            ptr += (size % Vector256<byte>.Count / sizeof(uint)) * sizeof(uint);

            // Since if all values are equal the MoveMask operation would return a value with all bits set, we are going
            // to negate it with the objective to do an OR or the XOR between the values. If there is any bit set, we know
            // we have different values.
            switch (size % sizeof(uint))
            {
                case 1:
                    // byte = 1
                    return (~matches | (uint)(*ptr ^ Unsafe.ReadUnaligned<byte>(ref constantRef))) == 0;
                case 2:
                    // ushort + byte = 2 
                    return (~matches | (uint)*(ushort*)ptr ^ Unsafe.ReadUnaligned<ushort>(ref constantRef)) == 0;
                case 3:
                    // ushort + byte = 2 + 1 = 3
                    return (
                        ~matches |
                        (uint)(*(ushort*)ptr ^ Unsafe.ReadUnaligned<ushort>(ref constantRef)) |
                        (uint)(*(ptr + sizeof(ushort)) ^ Unsafe.AddByteOffset(ref constantRef, sizeof(ushort)))
                    ) == 0;
            }

            throw new InvalidOperationException("If this happens, it is a bug.");

        Scalar:
            long scalarResult;
            switch (size % 16)
            {
                case 0:
                    return true;
                case 1:
                    // byte = 1
                    return *ptr == Unsafe.ReadUnaligned<byte>(ref constantRef);
                case 2:
                    // ushort + byte = 2 
                    // return *(ushort*)ptr == Unsafe.ReadUnaligned<ushort>(ref constantRef);
                    scalarResult = *(ushort*)ptr ^ Unsafe.ReadUnaligned<ushort>(ref constantRef);
                    break;
                case 3:
                    // ushort + byte = 2 + 1 = 3
                    scalarResult = (*(ushort*)ptr ^ Unsafe.ReadUnaligned<ushort>(ref constantRef)) |
                                   (*(ptr + sizeof(ushort)) ^ Unsafe.AddByteOffset(ref constantRef, sizeof(ushort)));
                    break;
                case 4:
                    // uint = 4 
                    scalarResult = *(uint*)ptr ^ Unsafe.ReadUnaligned<uint>(ref constantRef);
                    break;
                case 5:
                    // uint + byte = 4 + 1 = 5

                    scalarResult = (*(uint*)ptr ^ Unsafe.ReadUnaligned<uint>(ref constantRef)) |
                                   (*(ptr + sizeof(uint)) ^ (long)Unsafe.AddByteOffset(ref constantRef, sizeof(uint)));
                    break;
                case 6:
                    // uint + ushort = 4 + 2 = 6
                    scalarResult = (long)(*(uint*)ptr ^ Unsafe.ReadUnaligned<uint>(ref constantRef)) |
                                   (long)(*(ushort*)(ptr + sizeof(uint)) ^ Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(uint))));
                    break;
                case 7:
                    // uint + ushort + byte = 4 + 2 + 1 = 7
                    scalarResult = (long)(*(uint*)ptr ^ Unsafe.ReadUnaligned<uint>(ref constantRef)) |
                                   (long)(*(ushort*)(ptr + sizeof(uint)) ^ Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(uint)))) |
                                   (long)(*(ptr + sizeof(uint) + sizeof(ushort)) ^ Unsafe.AddByteOffset(ref constantRef, sizeof(uint) + sizeof(ushort)));
                    break;
                case 8:
                    // ulong = 8
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef));
                    break;
                case 9:
                    // ulong + byte = 8 + 1 = 9
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef)) |
                                   (long)(*(ptr + sizeof(ulong)) ^ Unsafe.AddByteOffset(ref constantRef, sizeof(ulong)));
                    break;
                case 10:
                    // ulong + ushort = 8 + 2 = 10
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef)) |
                                   (long)(*(ushort*)(ptr + sizeof(ulong)) ^ Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong))));
                    break;
                case 11:
                    // ulong + ushort + byte = 8 + 2 + 1 = 11
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef)) |
                                   (long)((uint)(*(ushort*)(ptr + sizeof(ulong)) ^ Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong)))) |
                                   (long)(*(ptr + sizeof(ulong) + sizeof(ushort)) ^ Unsafe.AddByteOffset(ref constantRef, sizeof(ulong) + sizeof(ushort))));
                    break;
                case 12:
                    // ulong + uint = 8 + 4 = 12
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef)) |
                                   (long)(*(uint*)(ptr + sizeof(ulong)) ^ Unsafe.ReadUnaligned<uint>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong))));
                    break;
                case 13:
                    // ulong + uint + byte = 8 + 4 + 1 = 13
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef)) |
                                   (long)(*(uint*)(ptr + sizeof(ulong)) ^ Unsafe.ReadUnaligned<uint>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong)))) |
                                   (long)(*(ptr + sizeof(ulong) + sizeof(uint)) ^ Unsafe.AddByteOffset(ref constantRef, sizeof(ulong) + sizeof(uint)));
                    break;
                case 14:
                    // ulong + uint + ushort = 8 + 4 + 2= 14
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef)) |
                                   (long)(*(uint*)(ptr + sizeof(ulong)) ^ Unsafe.ReadUnaligned<uint>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong)))) |
                                   (long)(*(ushort*)(ptr + sizeof(ulong) + sizeof(uint)) ^ Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong) + sizeof(uint))));
                    break;
                case 15:
                    // ulong + uint + ushort + byte = 8 + 4 + 2 + 1 = 15
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef)) |
                                   (long)(*(uint*)(ptr + sizeof(ulong)) ^ Unsafe.ReadUnaligned<uint>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong)))) |
                                   (long)(*(ushort*)(ptr + sizeof(ulong) + sizeof(uint)) ^ Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong) + sizeof(uint)))) |
                                   (long)(*(ptr + sizeof(ulong) + sizeof(uint) + sizeof(ushort)) ^ Unsafe.AddByteOffset(ref constantRef, sizeof(ulong) + sizeof(uint) + sizeof(ushort)));
                    break;
                default:
                    throw new InvalidOperationException("If this happens, it is a bug.");
            }

            return scalarResult == 0;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsEqualConstantVector128(ref byte constantRef, byte* ptr, int size)
        {
            if (size >= Vector128<byte>.Count)
            {
                Vector128<byte> result = Vector128.Equals(
                    Vector128.LoadUnsafe(ref constantRef),
                    Vector128.Load(ptr));

                constantRef = ref Unsafe.AddByteOffset(ref constantRef, Vector128<byte>.Count);
                ptr += Vector128<byte>.Count;

                if (size >= 2 * Vector128<byte>.Count)
                {
                    result = Vector128.BitwiseAnd(
                        result,
                        Vector128.Equals(
                            Vector128.LoadUnsafe(ref constantRef),
                            Vector128.Load(ptr)
                        ));

                    constantRef = ref Unsafe.AddByteOffset(ref constantRef, Vector128<byte>.Count);
                    ptr += Vector128<byte>.Count;
                }

                if (size >= 3 * Vector128<byte>.Count)
                {
                    result = Vector128.BitwiseAnd(
                        result,
                        Vector128.Equals(
                            Vector128.LoadUnsafe(ref constantRef),
                            Vector128.Load(ptr)
                        ));

                    constantRef = ref Unsafe.AddByteOffset(ref constantRef, Vector128<byte>.Count);
                    ptr += Vector128<byte>.Count;
                }

                if (size >= 4 * Vector128<byte>.Count)
                {
                    result = Vector128.BitwiseAnd(
                        result,
                        Vector128.Equals(
                            Vector128.LoadUnsafe(ref constantRef),
                            Vector128.Load(ptr)
                        ));

                    constantRef = ref Unsafe.AddByteOffset(ref constantRef, Vector128<byte>.Count);
                    ptr += Vector128<byte>.Count;
                }

                if (!Vector128.EqualsAll(result, Vector128<byte>.AllBitsSet))
                    return false;
            }

            long scalarResult;
            switch (size % 16)
            {
                case 0:
                    return true;
                case 1:
                    // byte = 1
                    return *ptr == Unsafe.ReadUnaligned<byte>(ref constantRef);
                case 2:
                    // ushort + byte = 2 
                    // return *(ushort*)ptr == Unsafe.ReadUnaligned<ushort>(ref constantRef);
                    scalarResult = *(ushort*)ptr ^ Unsafe.ReadUnaligned<ushort>(ref constantRef);
                    break;
                case 3:
                    // ushort + byte = 2 + 1 = 3
                    scalarResult = (*(ushort*)ptr ^ Unsafe.ReadUnaligned<ushort>(ref constantRef)) |
                                   (*(ptr + sizeof(ushort)) ^ Unsafe.AddByteOffset(ref constantRef, sizeof(ushort)));
                    break;
                case 4:
                    // uint = 4 
                    scalarResult = *(uint*)ptr ^ Unsafe.ReadUnaligned<uint>(ref constantRef);
                    break;
                case 5:
                    // uint + byte = 4 + 1 = 5

                    scalarResult = (*(uint*)ptr ^ Unsafe.ReadUnaligned<uint>(ref constantRef)) |
                                   (*(ptr + sizeof(uint)) ^ (long)Unsafe.AddByteOffset(ref constantRef, sizeof(uint)));
                    break;
                case 6:
                    // uint + ushort = 4 + 2 = 6
                    scalarResult = (long)(*(uint*)ptr ^ Unsafe.ReadUnaligned<uint>(ref constantRef)) |
                                   (long)(*(ushort*)(ptr + sizeof(uint)) ^ Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(uint))));
                    break;
                case 7:
                    // uint + ushort + byte = 4 + 2 + 1 = 7
                    scalarResult = (long)(*(uint*)ptr ^ Unsafe.ReadUnaligned<uint>(ref constantRef)) |
                                   (long)(*(ushort*)(ptr + sizeof(uint)) ^ Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(uint)))) |
                                   (long)(*(ptr + sizeof(uint) + sizeof(ushort)) ^ Unsafe.AddByteOffset(ref constantRef, sizeof(uint) + sizeof(ushort)));
                    break;
                case 8:
                    // ulong = 8
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef));
                    break;
                case 9:
                    // ulong + byte = 8 + 1 = 9
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef)) |
                                   (long)(*(ptr + sizeof(ulong)) ^ Unsafe.AddByteOffset(ref constantRef, sizeof(ulong)));
                    break;
                case 10:
                    // ulong + ushort = 8 + 2 = 10
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef)) |
                                   (long)(*(ushort*)(ptr + sizeof(ulong)) ^ Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong))));
                    break;
                case 11:
                    // ulong + ushort + byte = 8 + 2 + 1 = 11
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef)) |
                                   (long)((uint)(*(ushort*)(ptr + sizeof(ulong)) ^ Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong)))) |
                                   (long)(*(ptr + sizeof(ulong) + sizeof(ushort)) ^ Unsafe.AddByteOffset(ref constantRef, sizeof(ulong) + sizeof(ushort))));
                    break;
                case 12:
                    // ulong + uint = 8 + 4 = 12
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef)) |
                                   (long)(*(uint*)(ptr + sizeof(ulong)) ^ Unsafe.ReadUnaligned<uint>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong))));
                    break;
                case 13:
                    // ulong + uint + byte = 8 + 4 + 1 = 13
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef)) |
                                   (long)(*(uint*)(ptr + sizeof(ulong)) ^ Unsafe.ReadUnaligned<uint>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong)))) |
                                   (long)(*(ptr + sizeof(ulong) + sizeof(uint)) ^ Unsafe.AddByteOffset(ref constantRef, sizeof(ulong) + sizeof(uint)));
                    break;
                case 14:
                    // ulong + uint + ushort = 8 + 4 + 2= 14
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef)) |
                                   (long)(*(uint*)(ptr + sizeof(ulong)) ^ Unsafe.ReadUnaligned<uint>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong)))) |
                                   (long)(*(ushort*)(ptr + sizeof(ulong) + sizeof(uint)) ^ Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong) + sizeof(uint))));
                    break;
                case 15:
                    // ulong + uint + ushort + byte = 8 + 4 + 2 + 1 = 15
                    scalarResult = (long)(*(ulong*)ptr ^ Unsafe.ReadUnaligned<ulong>(ref constantRef)) |
                                   (long)(*(uint*)(ptr + sizeof(ulong)) ^ Unsafe.ReadUnaligned<uint>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong)))) |
                                   (long)(*(ushort*)(ptr + sizeof(ulong) + sizeof(uint)) ^ Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref constantRef, sizeof(ulong) + sizeof(uint)))) |
                                   (long)(*(ptr + sizeof(ulong) + sizeof(uint) + sizeof(ushort)) ^ Unsafe.AddByteOffset(ref constantRef, sizeof(ulong) + sizeof(uint) + sizeof(ushort)));
                    break;
                default:
                    throw new InvalidOperationException("If this happens, it is a bug.");
            }

            return scalarResult == 0;
        }
#endif


        [DllImport("libc", EntryPoint = "memcmp", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
        [SecurityCritical]
        private static extern int Compare_posix(byte* b1, byte* b2, long count);

        [DllImport("msvcrt.dll", EntryPoint = "memcmp", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
        [SecurityCritical]
        private static extern int Compare_windows(byte* b1, byte* b2, long count);

        private const int CompareInlineVsCallThreshold = 256;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int CompareInlineNet6OorLesser(void* p1, void* p2, int size)
        {
            // If we use an unmanaged bulk version with an inline compare the caller site does not get optimized properly.
            // If you know you will be comparing big memory chunks do not use the inline version. 
            if (size > CompareInlineVsCallThreshold)
                goto UnmanagedCompare;

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
            bpx += (long)Bits.TrailingZeroesInBytes(xor);
            return *bpx - *(bpx + offset);

            UnmanagedCompare:
            // This is the only place where sparrow calls direct pInvoke (replace when Unsafe.Compare/Buffer.Compare will be available)            
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return Compare_windows((byte*)p1, (byte*)p2, size);
            return Compare_posix((byte*)p1, (byte*)p2, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Compare(void* p1, void* p2, int size)
        {
            // Reference check is specially useful for certain comparisons.
            if (p1 == p2)
                return 0;

#if NET7_0_OR_GREATER
            if (AdvInstructionSet.X86.IsSupportedAvx256)
            {
                return CompareAvx256(p1, p2, size);
            }

            return new ReadOnlySpan<byte>(p1, size).SequenceCompareTo(new ReadOnlySpan<byte>(p2, size));
#else
            return CompareInlineNet6OorLesser(p1, p2, size);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(void* p1, void* p2, int size)
        {
            // Reference check is specially useful for certain comparisons.
            if (p1 == p2)
                return 0;

#if NET7_0_OR_GREATER
            if (AdvInstructionSet.X86.IsSupportedAvx256)
            {
                return CompareAvx256(p1, p2, size);
            }

            return new ReadOnlySpan<byte>(p1, size).SequenceCompareTo(new ReadOnlySpan<byte>(p2, size));
#else
            return CompareInlineNet6OorLesser(p1, p2, size);
#endif
        }

#if NET8_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(scoped ref readonly byte p1, scoped ref readonly byte p2, int size)
        {
            if (AdvInstructionSet.X86.IsSupportedAvx256)
            {
                return CompareAvx256(in p1, in p2, size);
            }

            return MemoryMarshal.CreateReadOnlySpan(in p1, size)
                .SequenceCompareTo(MemoryMarshal.CreateReadOnlySpan(in p2, size));
        }

#elif NET7_0_OR_GREATER

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(ref byte p1, ref byte p2, int size)
        {
            if (AdvInstructionSet.X86.IsSupportedAvx256)
            {
                return CompareAvx256(ref p1, ref p2, size);
            }

            return MemoryMarshal.CreateReadOnlySpan(ref p1, size)
                .SequenceCompareTo(MemoryMarshal.CreateReadOnlySpan(ref p2, size));
        }
#endif


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(ReadOnlySpan<byte> p1, ReadOnlySpan<byte> p2, int size)
        {
#if NET7_0_OR_GREATER
            ref byte p1Start = ref MemoryMarshal.GetReference(p1);
            ref byte p2Start = ref MemoryMarshal.GetReference(p2);
            if (AdvInstructionSet.X86.IsSupportedAvx256)
            {
                return CompareAvx256(ref p1Start, ref p2Start, size);
            }
#endif
            return p1.Slice(0, size).SequenceCompareTo(p2.Slice(0, size));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(void* dest, void* src, uint n)
        {
            Unsafe.CopyBlock(dest, src, n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(void* dest, void* src, long n)
        {
            if (n < uint.MaxValue) // Common code-path
            {
                Copy(dest, src, (uint)n);
                return;
            }
            
            CopyLong(dest, src, n);
        }

        private static void CopyLong(void* dest, void* src, long n)
        {
            for (long i = 0; i < n; i += uint.MaxValue)
            {
                var size = uint.MaxValue;
                if (i + uint.MaxValue > n)
                    size = (uint)(n % uint.MaxValue);
                Copy((byte*)dest + i, (byte*)src + i, size);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Set(byte* dest, byte value, uint n)
        {
            Unsafe.InitBlock(dest, value, n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Set(byte* dest, byte value, long n)
        {
            if (n < 0)
            {
                throw new NotSupportedException("You cannot pass negative values to mem set: " + n);
            }
            if (n < uint.MaxValue) // Common code-path
            {
                Set(dest, value, (uint)n);
                return;
            }
            
            SetLong(dest, value, n);
        }

        private static void SetLong(byte* dest, byte value, long n)
        {
            for (long i = 0; i < n; i += uint.MaxValue)
            {
                var size = uint.MaxValue;
                if (i + uint.MaxValue > n)
                    size = (uint)(n % uint.MaxValue);
                Set(dest + i, value, size);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Move(byte* dest, byte* src, int n)
        {
            // if dest and src overlaps, we need to call specifically to memmove (Buffer.MemoryCopy supports overlapping)
            if (dest + n >= src &&
                src + n >= dest)
            {
                Buffer.MemoryCopy(src, dest, n, n);
                return;
            }

            Copy(dest, src, n); // much faster if no overlapping
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyUnaligned(byte* dest, byte* src, uint n)
        {
            Unsafe.CopyBlockUnaligned(dest, src, n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TTo As<TFrom, TTo>(ref TFrom value)
        {
            return Unsafe.As<TFrom, TTo>(ref value);            
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T Read<T>(byte* ptr)
        {
            return Unsafe.Read<T>(ptr);
        }




        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsEqualConstant(ReadOnlySpan<byte> constant, byte* ptr)
        {
#if NET7_0_OR_GREATER
            // PERF: The intended use of this method is when the ReadOnlySpan<byte> is a constant, for anything
            // else the execution cost of this method will be horrible. The reason why is because when we are 
            // calling with a constant like `"this_is_a_string"u8.CompareConstant(somePtr)` the JIT compiler
            // will know the size of the caller and eliminate almost all the code and the branches
            // leaving very efficient code to be used by the caller. If there would be any way to know if the
            // span comes from a JIT known constant, we would throw or fallback to memory compare instead. 

            if (constant.Length >= 64)
                throw new NotSupportedException("The size must not be bigger than 64. The intended usage of this method is against constant short utf8 strings.");

            ref var constantRef = ref MemoryMarshal.GetReference(constant);

            if (AdvInstructionSet.X86.IsSupportedAvx256)
                return IsEqualConstantAvx256(ref constantRef, ptr, constant.Length);

            return IsEqualConstantVector128(ref constantRef, ptr, constant.Length);
#endif
            throw new NotSupportedException($"{nameof(IsEqualConstant)} is not supported in frameworks lesser than 7.0");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsEqualConstant(byte* constant, int size, byte* ptr)
        {
#if NET7_0_OR_GREATER
            // PERF: The intended use of this method is when the byte* size is a constant, for anything
            // else the execution cost of this method will be horrible. The reason why is that when we are 
            // calling with a constant sized pointer the JIT compiler will know the size of the caller and
            // eliminate almost all the code and the branches leaving very efficient code to be used by the
            // caller. If there would be any way to know if the span comes from a JIT known constant,
            // we would throw or fallback to memory compare instead. 

            if (size >= 64)
                throw new NotSupportedException("The size must not be bigger than 64. The intended usage of this method is against constant sized structures.");

            ref var constantRef = ref *constant;

            if (AdvInstructionSet.X86.IsSupportedAvx256)
                return IsEqualConstantAvx256(ref constantRef, ptr, size);

            return IsEqualConstantVector128(ref constantRef, ptr, size);
#else
            throw new NotSupportedException($"{nameof(IsEqualConstant)} is not supported in frameworks lesser than 7.0");
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsEqualConstant(ReadOnlySpan<byte> constant, byte* ptr, int size)
        {
#if NET7_0_OR_GREATER
            // PERF: The intended use of this method is when the ReadOnlySpan<byte> is a constant, for anything
            // else the execution cost of this method will be horrible. The reason why is that when we are 
            // calling with a constant like `"this_is_a_string"u8.CompareConstant(somePtr)` the JIT compiler
            // will know the size of the caller and eliminate almost all the code and the branches
            // leaving very efficient code to be used by the caller. If there would be any way to know if the
            // span comes from a JIT known constant, we would throw or fallback to memory compare instead. 

            if (constant.Length >= 64)
                throw new NotSupportedException("The size must not be bigger or equal to 64. The intended usage of this method is against constant short utf8 strings.");

            if (size != constant.Length)
                return false;

            ref var constantRef = ref MemoryMarshal.GetReference(constant);

            if (AdvInstructionSet.X86.IsSupportedAvx256)
                return IsEqualConstantAvx256(ref constantRef, ptr, constant.Length);

            return IsEqualConstantVector128(ref constantRef, ptr, constant.Length);
#else
            throw new NotSupportedException($"{nameof(IsEqualConstant)} is not supported in frameworks lesser than 7.0");
#endif
        }
    }
}
