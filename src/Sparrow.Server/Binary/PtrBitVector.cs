using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Text;
using static Sparrow.Binary.Bits;

namespace Sparrow.Binary
{
    internal readonly unsafe struct PtrBitVector
    {
        public readonly byte* Bits;
        public readonly int Count;

        public PtrBitVector(void* bits, int numberOfBits)
        {
            Bits = (byte*)bits;
            Count = numberOfBits;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool GetBitInPointer(void* ptr, int idx)
        {
            uint word = ByteForBit(idx);
            byte mask = BitInByte(idx);
            return (*((byte*)ptr + word) & mask) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SetBitInPointer(void* ptr, int idx, bool value)
        {
            uint word = ByteForBit(idx);
            byte mask = BitInByte(idx);

            byte* bytePtr = (byte*)ptr;
            bool currentValue = (bytePtr[word] & mask) != 0;
            if (currentValue != value)
                bytePtr[word] ^= mask;
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

            Bits[word] |= mask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte Set(int idx, bool value)
        {
            Contract.Requires(idx >= 0 && idx < Count);

            uint word = ByteForBit(idx);
            byte mask = BitInByte(idx);

            byte val = Bits[word];
            bool currentValue = (val & mask) != 0;
            if (currentValue != value)
                val ^= mask;

            Bits[word] = val;
            return val;
                
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Get(int idx)
        {
            Contract.Requires(idx >= 0 && idx < Count);

            uint word = ByteForBit(idx);
            byte mask = BitInByte(idx);
            return (Bits[word] & mask) != 0;
        }

        public string ToDebugString()
        {
            var builder = new StringBuilder();
            for (int i = 0; i < Count; i++)
                builder.Append(this[i] ? "1" : "0");

            return builder.ToString();
        }

        public bool AllEmpty()
        {
            if (Count == 256)
            {
                return Vector256.Load(Bits).Equals(Vector256<byte>.Zero);
            }

            return FirstSetBit() == -1;
        }

        public int FirstSetBit()
        {
            int lengthInBytes = (Count + 7) / 8; // Iterate in byte units

            int i = 0;
            for (; i + sizeof(ulong) <= lengthInBytes; i += sizeof(ulong))
            {
                ulong l = *(ulong*)(Bits + i);
                if (l != 0)
                    return ResolveIndex(i, l, Count);
            }

            int remaining = lengthInBytes - i;
            if (remaining > 0)
            {
                ulong word = 0;
                Unsafe.CopyBlockUnaligned(&word, Bits + i, (uint)remaining);
                if (word != 0)
                    return ResolveIndex(i, word, Count);
            }

            return -1;

            static int ResolveIndex(int byteOffset, ulong value, int count)
            {
                int idx = byteOffset * 8 + BitOperations.LeadingZeroCount(
                    // this is needed because we set on byte boundaries, but read using ulong
                    BinaryPrimitives.ReverseEndianness(value));
                return idx < count ? idx : -1; // avoid returning an index that is greater than the count (e.g. if the last byte is partially used)
            }
        }
    }
}
