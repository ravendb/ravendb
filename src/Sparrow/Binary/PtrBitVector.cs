using System;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using System.Text;

namespace Sparrow.Binary
{
    public unsafe struct PtrBitVector
    {
        public const int BitsPerByte = 8;
        public const int BitsPerWord = sizeof(ulong) * BitsPerByte; // 64
        public const int BytesPerWord = sizeof(ulong) / sizeof(byte); // 8

        public const uint Log2BitsPerByte = 3; // Math.Log( BitsPerByte, 2 )

        public readonly byte* Bits;
        public readonly int Count;

        public PtrBitVector(void* bits, int numberOfBits)
        {
            Bits = (byte*)bits;
            Count = numberOfBits;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint ByteForBit(int idx)
        {
            return (uint)(idx >> (int)Log2BitsPerByte);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte BitInByte(int idx)
        {
            // PERF: Will do the same thing using less bytes.
            //       For reference this is equivalent to [ 0x80 >> (idx % (int)BitsPerByte) ]
            return (byte)(0x80 >> (idx & (int)(BitsPerByte - 1)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool GetBitInPointer(void* ptr, int idx)
        {
            uint word = ByteForBit(idx);
            byte mask = BitInByte(idx);
            return (*((byte*)ptr + word) & mask) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool GetBitInSpan(Span<byte> ptr, int idx)
        {
            uint word = ByteForBit(idx);
            byte mask = BitInByte(idx);
            return (ptr[(int)word]& mask) != 0;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SetBitInSpan(Span<byte> bytePtr, int idx, bool value)
        {
            int word = (int)ByteForBit(idx);
            byte mask = BitInByte(idx);

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
        public void Set(int idx, bool value)
        {
            Contract.Requires(idx >= 0 && idx < Count);

            uint word = ByteForBit(idx);
            byte mask = BitInByte(idx);

            bool currentValue = (Bits[word] & mask) != 0;
            if (currentValue != value)
                Bits[word] ^= mask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Get(int idx)
        {
            Contract.Requires(idx >= 0 && idx < Count);

            uint word = ByteForBit(idx);
            byte mask = BitInByte(idx);
            return (Bits[word] & mask) != 0;
        }

        public int FindLeadingOne()
        {
            Debug.Assert(BitConverter.IsLittleEndian);

            ulong* ptr = (ulong*)Bits;
            int count = Count;
            int words = count / BitsPerWord;

            ulong value;
            int i;
            int idx;

            int accumulator = BitsPerWord;            
            for (i = 0; i < words; i++)
            {
                value = ptr[i];
                if (value == 0)
                {
                    accumulator += BitsPerWord;
                    continue;
                }

                if (BitConverter.IsLittleEndian)
                    value = Binary.Bits.SwapBytes(value);

                idx = accumulator - Binary.Bits.MostSignificantBit(value) - 1;
                return idx < count ? idx : -1;
            }

            value = 0;
            byte* bytePtr = Bits + words * BytesPerWord;

            // We want to know how many bytes we have left. 
            int bitsLeft = (count % BitsPerWord);
            int rotations = bitsLeft / BitsPerByte;
            if (bitsLeft % BitsPerByte != 0)
                rotations++;

            // We write the value and shift
            for (i = 0; i < rotations; i++)
            {
                value <<= BitsPerByte; // We shift first, because shifting 0 is still 0
                value |= bytePtr[i];
            }

            // We move the value as many places as we need to fill with zeroes.
            value <<= (BitsPerByte * (BytesPerWord - rotations));

            idx = accumulator - Binary.Bits.MostSignificantBit(value) - 1;
            return idx < count ? idx : -1;
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
