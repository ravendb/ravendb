using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Binary
{
    public class BitVector
    {
        public const uint Log2BitsPerWord = 6; // Math.Log( sizeof(long), 2 )
	    public const uint BitsPerWord = sizeof(ulong);
	    public const uint WordMask = BitsPerWord - 1;
        public const uint LastBit = BitsPerWord - 1;
	    public const ulong Ones = 0xFFFFFFFFFFFFFFFFL;
	    public const ulong LastBitMask = 1UL << (int)LastBit;

        private ulong[] bits;

        public BitVector(int size)
        {
            this.Count = size;
            this.bits = new ulong[size % sizeof(ulong) == 0 ? size * sizeof(ulong) : size * sizeof(ulong) + 1];            
        }

        public int Count
        {
            get;
            private set;
        }

        public bool this[int idx]
        {
            get { return Get(idx); }
            set { Set(idx, value); }
        }

        public void Set(int idx)
        {
            int word = Word(idx);
            ulong mask = Mask(idx);

            bits[word] |= mask;                
        }

        public void Set(int idx, bool value)
        {
            int word = Word(idx);
            ulong mask = Mask(idx);

            bool currentValue = (bits[word] & mask) != 0;
            if (currentValue != value)
                bits[word] ^= mask;
        }

        public bool Get(int idx)
        {
            int word = Word(idx);
            ulong mask = Mask(idx);
            return (bits[word] & mask) != 0;
        }

        public ulong GetWord(int wordIdx)
        {
            return bits[wordIdx];
        }

        public void Fill(bool value)
        {
            unsafe
            {
                byte x = value ? (byte)0xFF : (byte)0x00;
                fixed (ulong* array = this.bits)
                    Memory.SetInline((byte*)array, x, this.bits.Length);
            }
        }

        public void Fill(bool value, int from, int to)
        {
            unchecked
            {
                int bFrom = from / sizeof(long);
                int bTo = to / sizeof(long);

                if (bFrom == bTo)
                {
                    ulong fill = (1UL << (to - from) - 1 << from);
                    if (value)
                        bits[bFrom] |= fill;
                    else
                        bits[bFrom] &= ~fill;
                }
                else
                {
                    byte x = value ? (byte)0xFF : (byte)0x00;
                    unsafe
                    {
                        fixed (ulong* array = this.bits)
                            Memory.SetInline((byte*)(array + bFrom + 1), x, bTo);
                    }

                    if (from % sizeof(long) != 0)
                    {
                        if (value)
                            bits[bFrom] |= (ulong)(-1L) << from % sizeof(long);
                        else
                            bits[bFrom] &= (1UL << from % sizeof(long)) - 1;
                    }

                    if (to % sizeof(long) != 0)
                    {
                        if (value)
                            bits[bTo] |= 1UL << to % sizeof(long) - 1;
                        else
                            bits[bTo] &= (ulong)(-1L) << to % sizeof(long);
                    }
                }
            }
        }

        public void Flip()
        {
            throw new NotImplementedException();
        }

        public void Flip(int idx)
        {
            throw new NotImplementedException();
        }

        public void Flip(int from, int to)
        {
            throw new NotImplementedException();
        }

        public void Clear()
        {
            Array.Clear(this.bits, 0, this.bits.Length);
        }

        public BitVector And(BitVector v)
        {
            throw new NotImplementedException();
        }

        public BitVector Or(BitVector v)
        {
            throw new NotImplementedException();
        }

        public BitVector Xor(BitVector v)
        {
            throw new NotImplementedException();
        }

        public void CopyTo(BitVector dest)
        {
            throw new NotImplementedException();
        }

        public static void Copy(BitVector src, BitVector dest)
        {
            throw new NotImplementedException();
        }

        public static BitVector OfLength(int size)
        {
            return new BitVector(size);
        }

        protected static ulong Mask(int idx)
        {
            throw new NotImplementedException();
        }

        protected static int Bit (int idx)
        {
            throw new NotImplementedException();
        }

        protected static int Word (int idx)
        {
            throw new NotImplementedException();
        }

        protected static int NumberOfWords(int size)
        {
            throw new NotImplementedException();
        }
    }
}
