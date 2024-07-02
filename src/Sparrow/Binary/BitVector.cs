using System;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using static Sparrow.Binary.Bits;

#if NET7_0_OR_GREATER
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
#endif

namespace Sparrow.Binary
{
    internal readonly ref struct BitVector
    {
        public const int BitsPerByte = 8;
        public const int BitsPerWord = sizeof(ulong) * BitsPerByte; // 64
        public const int BytesPerWord = sizeof(ulong) / sizeof(byte); // 8

        public const uint Log2BitsPerByte = 3; // Math.Log( BitsPerByte, 2 )

        private readonly Span<byte> _storage;
        public int Count => _storage.Length * 8;

        public BitVector(Span<byte> storage)
        {
            _storage = storage;
        }

        public bool this[int idx]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Get(idx); }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Set(idx, value); }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(int idx)
        {
            Contract.Requires(idx >= 0 && idx < Count);

            uint word = ByteForBit(idx);
            byte mask = BitInByte(idx);

            ref byte currentWord = ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(_storage), word);
            currentWord |= mask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(int idx, bool value)
        {
            Contract.Requires(idx >= 0 && idx < Count);

            uint word = ByteForBit(idx);
            byte mask = BitInByte(idx);

            ref byte currentWord = ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(_storage), word);

            bool currentValue = (currentWord & mask) != 0;
            if (currentValue != value)
                currentWord ^= mask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Get(int idx)
        {
            Contract.Requires(idx >= 0 && idx < Count);

            uint word = ByteForBit(idx);
            byte mask = BitInByte(idx);
            return (Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(_storage), word) & mask) != 0;
        }

#if NET7_0_OR_GREATER
        internal static int IndexOfFirstSetBitAvx2(ref byte storage, int lengthInBytes)
        {
            int N = Vector256<byte>.Count;

            nint index = 0;
            for (; index + N <= lengthInBytes; index += N)
            {
                var input = Vector256.LoadUnsafe(ref Unsafe.AddByteOffset(ref storage, index));
                var comparison = Vector256.GreaterThan(input, Vector256<byte>.Zero);
                var result = (uint)Avx2.MoveMask(comparison);
                if (result == 0)
                    continue;

                index += LeadingZeroes(result);
                goto Done;
            }

            for (; index < lengthInBytes; index++)
            {
                if (Unsafe.AddByteOffset(ref storage, index) != 0)
                    goto Done;
            }

            return -1;

            Done:
            var value = Unsafe.AddByteOffset(ref storage, index) << ((sizeof(int) - 1) * 8);
            value = LeadingZeroes((uint)value);
            return (value < 8) ? (int)index * 8 + value : - 1;
        }

        internal static int IndexOfFirstSetBitSse2(ref byte storage, int lengthInBytes)
        {
            int N = Vector128<byte>.Count;

            nint index = 0;
            for (; index + N <= lengthInBytes; index += N)
            {
                var input = Vector128.LoadUnsafe(ref Unsafe.AddByteOffset(ref storage, index));
                var comparison = Vector128.GreaterThan(input, Vector128<byte>.Zero);
                var result = (uint)Sse2.MoveMask(comparison);
                if (result == 0)
                    continue;

                index += LeadingZeroes(result);
                goto Done;
            }

            for (; index < lengthInBytes; index++)
            {
                if (Unsafe.AddByteOffset(ref storage, index) != 0)
                    goto Done;
            }

            return -1;

            Done:
            var value = Unsafe.AddByteOffset(ref storage, index) << ((sizeof(int) - 1) * 8);
            value = LeadingZeroes((uint)value);
            return (value < 8) ? (int)index * 8 + value : -1;
        }
#endif

        internal static int IndexOfFirstSetBitScalar(ref byte storage, int lengthInBytes)
        {
            int N = sizeof(long);
            int value;

            var index = 0;
            for (; index + N <= lengthInBytes; index += N)
            {
                var input = Unsafe.ReadUnaligned<long>(ref Unsafe.AddByteOffset(ref storage, (nuint)index));
                if (input == 0)
                    continue;

                value = LeadingZeroes(input);
                return (value < sizeof(long) * 8) ? (int)index * 8 + value : -1;
            }

            for (; index < lengthInBytes; index++)
            {
                if (Unsafe.AddByteOffset(ref storage, (nuint)index) != 0)
                    goto Done;
            }

            return -1;

            Done:
            value = Unsafe.AddByteOffset(ref storage, (nuint)index) << ((sizeof(int) - 1) * 8);
            value = LeadingZeroes((uint)value);
            return (value < 8) ? (int)index * 8 + value : -1;
        }

        public int IndexOfFirstSetBit()
        {
            ref var storage = ref MemoryMarshal.GetReference(_storage);
#if NET7_0_OR_GREATER
            if (AdvInstructionSet.X86.IsSupportedAvx256)
                return IndexOfFirstSetBitAvx2(ref storage, _storage.Length);
            if (AdvInstructionSet.X86.IsSupportedSse)
                return IndexOfFirstSetBitSse2(ref storage, _storage.Length);
#endif
            return IndexOfFirstSetBitScalar(ref storage, _storage.Length);
        }

        public string ToDebugString()
        {
            var builder = new StringBuilder();
            for (int i = 0; i < Count; i++)
                builder.Append(this[i] ? "1" : "0");

            return builder.ToString();
        }
    }
}
