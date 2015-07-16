using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Binary
{

    /// <summary>
    /// Differently from the numeric representation a BitVector operation is left-aligned.
    /// </summary>
    [DebuggerDisplay("{ToDebugString()}")]
    public class BitVector : IComparable<BitVector>
    {
        public const int BitsPerByte = 8;
        public const int BitsPerWord = sizeof(ulong) * BitsPerByte;        
        public const uint Log2BitsPerWord = 6; // Math.Log( BitsPerWord, 2 )
        
	    public const uint WordMask = BitsPerWord - 1;
        public const ulong Ones = 0xFFFFFFFFFFFFFFFFL;

        public const uint FirstBitPosition = BitsPerWord - 1;
        public const ulong FirstBitMask = 1UL << (int)FirstBitPosition;

        public const ulong LastBitMask = 1UL;
        public const uint LastBitPosition = 1;
        
        public readonly ulong[] Bits;        

        protected string ToDebugString()
        {
            unchecked
            {
                var builder = new StringBuilder();
                if (this.Count <= BitsPerWord)
                {
                    for (int i = 0; i < this.Count; i++)
                        builder.Append(this[i] ? 1 : 0);
                }
                else
                {
                    int words = NumberOfWordsForBits(this.Count);
                    for (int i = 0; i < words; i++)
                    {
                        ulong v = this.GetWord(i);

                        builder.Append(v.ToString("X16"));
                        builder.Append(" ");
                    }
                }

                return builder.ToString();
            }            
        }

        private static ulong Reverse(ulong v)
        {
            int s = BitsPerWord; // bit size; must be power of 2 

            unchecked
            {
                ulong mask = (ulong)~0;
                while ((s >>= 1) > 0)
                {
                    mask ^= (mask << s);
                    v = ((v >> s) & mask) | ((v << s) & ~mask);
                }
            }

            return v;
        }

        public BitVector(int size)
        {
            this.Count = size;
            this.Bits = new ulong[size % BitVector.BitsPerWord == 0 ? size / BitVector.BitsPerWord : size / BitVector.BitsPerWord + 1];            
        }

        protected BitVector(int size, params ulong[] values)
        {
            if (size / BitVector.BitsPerWord > values.Length)
                throw new ArgumentException("The values passed as parameters does not have enough bits to fill the vector size.", "values");

            this.Count = size;    
            this.Bits = values;
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
            uint word = WordForBit(idx);
            ulong mask = BitInWord(idx);

            Bits[word] |= mask;                
        }

        public void Set(int idx, bool value)
        {
            uint word = WordForBit(idx);
            ulong mask = BitInWord(idx);

            bool currentValue = (Bits[word] & mask) != 0;
            if (currentValue != value)
                Bits[word] ^= mask;
        }

        public bool Get(int idx)
        {
            uint word = WordForBit(idx);
            ulong mask = BitInWord(idx);
            return (Bits[word] & mask) != 0;
        }

        public ulong GetWord(int wordIdx)
        {
            return Bits[wordIdx];
        }

        public void Fill(bool value)
        {
            unsafe
            {
                byte x = value ? (byte)0xFF : (byte)0x00;
                fixed (ulong* array = this.Bits)
                    Memory.SetInline((byte*)array, x, this.Bits.Length * sizeof(ulong));
            }
        }

        public void Fill(bool value, int from, int to)
        {
            unchecked
            {
                int bFrom = from / BitVector.BitsPerWord;
                int bTo = to / BitVector.BitsPerWord;

                if (bFrom == bTo)
                {
                    ulong fill = (1UL << (to - from) - 1 << from);
                    if (value)
                        Bits[bFrom] |= fill;
                    else
                        Bits[bFrom] &= ~fill;
                }
                else
                {
                    byte x = value ? (byte)0xFF : (byte)0x00;
                    unsafe
                    {
                        fixed (ulong* array = this.Bits)
                            Memory.SetInline((byte*)(array + bFrom + 1), x, bTo);
                    }

                    if (from % BitVector.BitsPerWord != 0)
                    {
                        if (value)
                            Bits[bFrom] |= (ulong)(-1L) << from % BitVector.BitsPerWord;
                        else
                            Bits[bFrom] &= (1UL << from % BitVector.BitsPerWord) - 1;
                    }

                    if (to % BitVector.BitsPerWord != 0)
                    {
                        if (value)
                            Bits[bTo] |= 1UL << to % BitVector.BitsPerWord - 1;
                        else
                            Bits[bTo] &= (ulong)(-1L) << to % BitVector.BitsPerWord;
                    }
                }
            }
        }

        public void Flip()
        {
            for (int i = 0; i < Bits.Length; i++)
                Bits[i] ^= BitVector.Ones;
        }

        public void Flip(int idx)
        {
            unchecked
            {
                uint wPos = WordForBit(idx);
                Bits[wPos] ^= BitInWord((int)idx);
            }
        }

        public void Flip(int from, int to)
        {
            unchecked
            {
                int bTo = to / BitVector.BitsPerWord;
                int bFrom = from / BitVector.BitsPerWord;
                if (bTo == bFrom)
                {
                    if (from == to)
                    {
                        Bits[bFrom] ^= BitInWord((int)from);
                    }
                    else
                    {
                        ulong mask = Ones << BitVector.BitsPerWord - (to - from);
                        Bits[bFrom] ^= mask >> from;
                    }
                        
                }
                else
                {
                    int start = (from + BitVector.BitsPerWord - 1) / BitVector.BitsPerWord;

                    ulong mask = BitVector.Ones;
                    for (int i = bTo; i-- != start; )
                        Bits[i] ^= mask;

                    if (from % BitVector.BitsPerWord != 0)
                        Bits[bFrom] ^= (1UL << (BitVector.BitsPerWord - from) % BitVector.BitsPerWord) - 1;
                    if (to % BitVector.BitsPerWord != 0)
                        Bits[bTo] ^= Ones << BitVector.BitsPerWord - (to % BitVector.BitsPerWord);
                }
            }
        }

        public void Clear()
        {
            Array.Clear(this.Bits, 0, this.Bits.Length);
        }

        public BitVector And(BitVector v)
        {
            int words = Math.Min(Bits.Length, v.Bits.Length) - 1;
            while (words >= 0)
            {
                Bits[words] &= v.Bits[words];

                words--;
            }

            return this;
        }

        public BitVector Or(BitVector v)
        {
            int words = Math.Min(Bits.Length, v.Bits.Length) - 1;
            while (words >= 0)
            {
                Bits[words] |= v.Bits[words];

                words--;
            }

            return this;
        }

        public BitVector Xor(BitVector v)
        {
            int words = Math.Min(Bits.Length, v.Bits.Length) - 1;
            while (words >= 0)
            {
                Bits[words] ^= v.Bits[words];

                words--;
            }

            return this;
        }

        public static BitVector And(BitVector x, BitVector y)
        {
            if (x.Count < y.Count)
            {
                var t = new BitVector(x.Count);
                x.CopyTo(t);
                return t.And(y);
            }
            else
            {
                var t = new BitVector(y.Count);
                y.CopyTo(t);
                return t.And(x);
            }
        }

        public static BitVector Or(BitVector x, BitVector y)
        {
            if (x.Count < y.Count)
            {
                var t = new BitVector(x.Count);
                x.CopyTo(t);
                return t.Or(y);
            }
            else
            {
                var t = new BitVector(y.Count);
                y.CopyTo(t);
                return t.Or(x);
            }
        }

        public static BitVector Xor(BitVector x, BitVector y)
        {
            if (x.Count < y.Count)
            {
                var t = new BitVector(x.Count);
                x.CopyTo(t);
                return t.Xor(y);
            }
            else
            {
                var t = new BitVector(y.Count);
                y.CopyTo(t);
                return t.Xor(x);
            }
        }


        public void CopyTo(BitVector dest)
        {
            Copy(this, dest);
        }

        public static void Copy(BitVector src, BitVector dest)
        {
            Contract.Requires(src.Count <= dest.Count);

            dest.Count = src.Count;

            unsafe
            {
                fixed (ulong* destPtr = dest.Bits)
                fixed (ulong* srcPtr = src.Bits)
                {
                    Memory.CopyInline((byte*)destPtr, (byte*)srcPtr, src.Bits.Length * sizeof(ulong));
                }
            }
        }

        public static BitVector OfLength(int size)
        {
            return new BitVector(size);
        }

        public static BitVector Of(params ulong[] values)
        {
            return new BitVector(values.Length * BitVector.BitsPerWord, values);
        }

        public static BitVector Of(params long[] values)
        {
            ulong[] newValue = new ulong[values.Length];
            for( int i = 0; i < values.Length; i++ )
                newValue[i] = (ulong) values[i];

            return new BitVector(values.Length * BitVector.BitsPerWord, newValue);
        }

        public static BitVector Of(params uint[] values)
        {
            int lastLong = values.Length / 2;
            int size = lastLong + values.Length % 2;

            ulong[] newValue = new ulong[size];
            for (int i = 0; i < lastLong; i++)
                newValue[i] = (ulong)values[2 * i] << 32 | (ulong)values[2 * i + 1];

            if (values.Length % 2 == 1)
                newValue[newValue.Length - 1] = (ulong)values[values.Length - 1] << 32;

            return new BitVector(values.Length * BitVector.BitsPerWord / 2, newValue);
        }

        public static BitVector Of(params byte[] values)
        {
            int extraBytes = values.Length % sizeof(ulong);
            int lastLong = values.Length / sizeof(ulong);

            int size = lastLong + ((extraBytes == 0) ? 0 : 1);

            int position;
            ulong[] newValue = new ulong[size];
            for (int i = 0; i < lastLong; i++)
            {
                position = i * sizeof(ulong);
                newValue[i] = (ulong)values[position] << 64 - 8 | 
                              (ulong)values[position + 1] << 64 - 16 | 
                              (ulong)values[position + 2] << 64 - 24 |
                              (ulong)values[position + 3] << 32 |
                              (ulong)values[position + 4] << 24 |
                              (ulong)values[position + 5] << 16 | 
                              (ulong)values[position + 6] << 8 |
                              (ulong)values[position + 7];
            }

            if ( extraBytes != 0 )
            {
                position = lastLong * sizeof(ulong);

                int bytesLeft = extraBytes;
                ulong lastValue = 0;
                do
                {
                    lastValue = lastValue << 8 | values[position];

                    position++;
                    bytesLeft--;
                }
                while (bytesLeft > 0);

                newValue[size - 1] = lastValue << (8 - extraBytes) * BitVector.BitsPerByte;
            }

            return new BitVector(values.Length * BitVector.BitsPerByte, newValue);
        }

        public static BitVector Of(string value)
        {
            int lastLong = value.Length / 4;
            int size = lastLong;
            if (value.Length % 4 != 0)
                size++;

            int position;
            ulong[] newValue = new ulong[size];
            for (int i = 0; i < lastLong; i++)
            {
                position = i * 4;
                newValue[i] = (ulong)value[position] << 48 | (ulong)value[position + 1] << 32 | (ulong)value[position + 2] << 16 | (ulong)value[position + 3];
            }

            position = (newValue.Length - 1) * 4;
            switch( value.Length % 4)
            {
                case 3: newValue[newValue.Length - 1] = (ulong)value[position] << 48 | (ulong)value[position + 1] << 32 | (ulong)value[position + 2] << 16; break;
                case 2: newValue[newValue.Length - 1] = (ulong)value[position] << 48 | (ulong)value[position + 1] << 32; break;
                case 1: newValue[newValue.Length - 1] = (ulong)value[position] << 48; break;
                default: break;
            }

            return new BitVector(value.Length * BitVector.BitsPerWord / 4, newValue);
                
        }

        public static BitVector Parse(string value)
        {
            var vector = new BitVector(value.Length);

            for (int i = 0; i < value.Length; i++ )
            {
                if (value[i] != '0')
                    vector[i] = true;
            }

            return vector;
        }

        public static ulong BitInWord(int idx)
        {
            return 0x8000000000000000UL >> (idx % (int)BitVector.BitsPerWord);
        }

        public static uint Bit(int idx)
        {
            Contract.Requires(idx < BitVector.BitsPerWord);

            return WordMask & (uint)idx;
        }

        public static uint WordForBit(int idx)
        {
            return (uint)(idx >> (int)Log2BitsPerWord);
        }

        public static int NumberOfWordsForBits(int size)
        {
            return (int)((size + WordMask) >> (int)Log2BitsPerWord);
        }

        public int CompareTo(BitVector other)
        {
            int bits;
            return CompareToInline(other, out bits);
        }

        public int CompareTo(BitVector other, out int equalBits)
        {
            return CompareToInline(other, out equalBits);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareToInline(BitVector other, out int equalBits)
        {
            var srcKey = this.Count;
            var otherKey = other.Count;
            var length = Math.Min(srcKey, otherKey);

            unsafe
            {
                fixed (ulong* srcPtr = this.Bits)
                fixed (ulong* destPtr = other.Bits)
                {
                    int wholeBytes = length / BitsPerWord;
                    int index;

                    ulong* bpx = srcPtr;
                    ulong* bpy = destPtr;
                    int last = 0;
                    for (int i = 0; i < wholeBytes; i++, bpx += 1, bpy += 1)
                    {
                        if (*((long*)bpx) != *((long*)bpy))
                            break;
                    }

                TAIL:
                    // We always finish the last extent with a bit-wise comparison (bit vector is stored in big endian).
                    int from = (int)(bpx - srcPtr) * BitsPerWord;
                    int leftover = length - from;
                    while (leftover > 0)
                    {
                        // TODO: We can try use a fast Rank0 function to find leading zeroes after an XOR to achieve peak performance at the byte level. 
                        //       See Broadword Implementation of Rank/Select Queries by Sebastiano Vigna http://vigna.di.unimi.it/ftp/papers/Broadword.pdf
                        bool thisBit = this[from];
                        bool otherBit = other[from];

                        if (thisBit != otherBit)
                        {
                            equalBits = from;
                            return thisBit ? 1 : -1;
                        }                            

                        from++;
                        leftover--;
                    }                    
                }
            }

            equalBits = length;
            return srcKey - otherKey;
        }

        public int LongestCommonPrefixLength(BitVector other)
        {           
            int differentBit;
            CompareToInline( other, out differentBit );

            return differentBit;
        }

        public BitVector SubVector(int start, int lenght)
        {
            throw new NotImplementedException();
        }
    }
}
