using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Sparrow.Binary
{
    public unsafe struct PtrBitVector
    {
        public const uint BitsPerByte = 8;
        public const uint Log2BitsPerByte = 3; // Math.Log( BitsPerByte, 2 )

        public readonly byte* Bits;
        public readonly int Count;

        public PtrBitVector(void* bits, int numberOfBits)
        {
            this.Bits = (byte*)bits;
            this.Count = numberOfBits;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint ByteForBit(int idx)
        {
            return (uint)(idx >> (int)Log2BitsPerByte);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte BitInByte(int idx)
        {
            return (byte)(0x80 >> (idx % (int)BitsPerByte));
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
            Contract.Requires(idx >= 0 && idx < this.Count);

            uint word = ByteForBit(idx);
            byte mask = BitInByte(idx);

            Bits[word] |= mask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(int idx, bool value)
        {
            Contract.Requires(idx >= 0 && idx < this.Count);

            uint word = ByteForBit(idx);
            byte mask = BitInByte(idx);

            bool currentValue = (Bits[word] & mask) != 0;
            if (currentValue != value)
                Bits[word] ^= mask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Get(int idx)
        {
            Contract.Requires(idx >= 0 && idx < this.Count);

            uint word = ByteForBit(idx);
            byte mask = BitInByte(idx);
            return (Bits[word] & mask) != 0;
        }

        public int FindLeadingOne()
        {
            Debug.Assert(BitConverter.IsLittleEndian);

            ulong* ptr = (ulong*)Bits;
            int count = this.Count;
            int words = count / BitVector.BitsPerWord;

            ulong value;
            int i;
            int idx;

            int accumulator = BitVector.BitsPerWord;            
            for (i = 0; i < words; i++)
            {
                value = ptr[i];
                if (value == 0)
                {
                    accumulator += BitVector.BitsPerWord;
                    continue;
                }

                if (BitConverter.IsLittleEndian)
                    value = Binary.Bits.SwapBytes(value);

                idx = accumulator - Binary.Bits.MostSignificantBit(value) - 1;
                return idx < count ? idx : -1;
            }

            value = 0;
            byte* bytePtr = Bits + words * BitVector.BytesPerWord;

            // We want to know how many bytes we have left. 
            int bitsLeft = (count % BitVector.BitsPerWord);
            int rotations = bitsLeft / BitVector.BitsPerByte;
            if (bitsLeft % BitVector.BitsPerByte != 0)
                rotations++;

            // TODO: Can we just write it in Little Endian Format (aka in reverse order)?

            // We write the value and shift
            for (i = 0; i < rotations; i++)
            {
                value <<= BitVector.BitsPerByte; // We shift first, because shifting 0 is still 0
                value |= bytePtr[i];
            }

            // We move the value as many places as we need to fill with zeroes.
            value <<= (BitVector.BitsPerByte * (BitVector.BytesPerWord - rotations));

            idx = accumulator - Binary.Bits.MostSignificantBit(value) - 1;
            return idx < count ? idx : -1;
        }
    }
}
