using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Sparrow.Binary
{
    public unsafe struct PtrBitVector
    {
        public readonly ulong* Bits;
        public readonly int Count;

        public PtrBitVector(ulong* bits, int numberOfBits)
        {
            this.Bits = bits;
            this.Count = numberOfBits;
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

            uint word = BitVector.WordForBit(idx);
            ulong mask = BitVector.BitInWord(idx);

            Bits[word] |= mask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(int idx, bool value)
        {
            Contract.Requires(idx >= 0 && idx < this.Count);

            uint word = BitVector.WordForBit(idx);
            ulong mask = BitVector.BitInWord(idx);

            bool currentValue = (Bits[word] & mask) != 0;
            if (currentValue != value)
                Bits[word] ^= mask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Get(int idx)
        {
            Contract.Requires(idx >= 0 && idx < this.Count);

            uint word = BitVector.WordForBit(idx);
            ulong mask = BitVector.BitInWord(idx);
            return (Bits[word] & mask) != 0;
        }

        public int FindLeadingOne()
        {
            int idx;
            if (this.Count <= BitVector.BitsPerWord)
            {
                if (Bits[0] == 0)
                    return -1;

                idx = BitVector.BitsPerWord - Binary.Bits.MostSignificantBit(Bits[0]) - 1;
                return idx < this.Count ?  idx : -1;
            }                
            else
            {
                int accumulator = BitVector.BitsPerWord;
                for (int i = 0; i <= this.Count / BitVector.BitsPerWord; i++ )
                {
                    if (Bits[i] == 0)
                    {
                        accumulator += BitVector.BitsPerWord;
                        continue;
                    }

                    idx = accumulator - Binary.Bits.MostSignificantBit(Bits[i]) - 1;
                    return idx < this.Count ? idx : -1;
                }
            }

            return -1;
        }
    }
}
