using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Sparrow.Server.Strings
{
    public static class StringsExtensions
    {

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
        internal static unsafe bool CompareConstantAvx2(ref byte constantRef, byte* ptr, int size)
        {
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
        internal static unsafe bool CompareConstantVector128(ref byte constantRef, byte* ptr, int size)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe bool CompareConstant(this ReadOnlySpan<byte> constant, byte* ptr)
        {
            // PERF: The intended use of this method is when the ReadOnlySpan<byte> is a constant, for anything
            // else the execution cost of this method will be horrible. The reason why is because when we are 
            // calling with a constant like `"this_is_a_string"u8.CompareConstant(somePtr)` the JIT compiler
            // will know the size of the caller and eliminate almost all the code and the branches
            // leaving very efficient code to be used by the caller. If there would be any way to know if the
            // span comes from a JIT known constant, we would throw or fallback to memory compare instead. 

            if (constant.Length > 64)
                throw new NotSupportedException("The size must not be bigger than 64. The intended usage of this method is against constant short utf8 strings.");

            ref var constantRef = ref MemoryMarshal.GetReference(constant);

            if (Avx2.IsSupported)
                return CompareConstantAvx2(ref constantRef, ptr, constant.Length);

            return CompareConstantVector128(ref constantRef, ptr, constant.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe bool CompareConstant(this ReadOnlySpan<byte> constant, byte* ptr, int size)
        {
            // PERF: The intended use of this method is when the ReadOnlySpan<byte> is a constant, for anything
            // else the execution cost of this method will be horrible. The reason why is because when we are 
            // calling with a constant like `"this_is_a_string"u8.CompareConstant(somePtr)` the JIT compiler
            // will know the size of the caller and eliminate almost all the code and the branches
            // leaving very efficient code to be used by the caller. If there would be any way to know if the
            // span comes from a JIT known constant, we would throw or fallback to memory compare instead. 

            if (constant.Length > 64)
                throw new NotSupportedException("The size must not be bigger than 64. The intended usage of this method is against constant short utf8 strings.");

            if (size != constant.Length)
                return false;

            ref var constantRef = ref MemoryMarshal.GetReference(constant);

            if (Avx2.IsSupported)
                return CompareConstantAvx2(ref constantRef, ptr, constant.Length);

            return CompareConstantVector128(ref constantRef, ptr, constant.Length);
        }
    }
}
