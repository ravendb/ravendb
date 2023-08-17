using System;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Sparrow.Binary
{
    internal static class Bits
    {
        public const int InByte = 8;
        public const int InShort = 16;
        public const int InInt = 32;
        public const int InLong = 64;

        private const int GB = 1024 * 1024 * 1024;

        //https://stackoverflow.com/questions/2709430/count-number-of-bits-in-a-64-bit-long-big-integer
        public static long NumberOfSetBits(long i)
        {
#if NET6_0_OR_GREATER
            return BitOperations.PopCount((ulong)i);
#else
            i = i - ((i >> 1) & 0x5555555555555555);
            i = (i & 0x3333333333333333) + ((i >> 2) & 0x3333333333333333);
            return (((i + (i >> 4)) & 0xF0F0F0F0F0F0F0F) * 0x101010101010101) >> 56;
#endif
        }


        // Code taken from http://graphics.stanford.edu/~seander/bithacks.html#IntegerLogDeBruijn

        private static readonly byte[] MultiplyDeBruijnBitPosition = 
                {
                    0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30,
                    8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31
                };

        private static readonly byte[] DeBruijnBytePos64 = 
            {
                0, 0, 0, 0, 0, 1, 1, 2, 0, 3, 1, 3, 1, 4, 2, 7, 0, 2, 3, 6, 1, 5, 3, 5, 1, 3, 4, 4, 2, 5, 6, 7,
                7, 0, 1, 2, 3, 3, 4, 6, 2, 6, 5, 5, 3, 4, 5, 6, 7, 1, 2, 4, 6, 4, 4, 5, 7, 2, 6, 5, 7, 6, 7, 7
            };

        private static readonly byte[] DeBruijnBytePos32 = 
            {
                0, 0, 3, 0, 3, 1, 3, 0, 3, 2, 2, 1, 3, 2, 0, 1, 3, 3, 1, 2, 2, 2, 2, 0, 3, 1, 2, 0, 1, 0, 1, 1
            };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int MostSignificantBit(uint n)
        {
            n |= n >> 1; // first round down to one less than a power of 2 
            n |= n >> 2;
            n |= n >> 4;
            n |= n >> 8;
            n |= n >> 16;

            return MultiplyDeBruijnBitPosition[(n * 0x07C4ACDDU) >> 27];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int MostSignificantBit(int n)
        {
            n |= n >> 1; // first round down to one less than a power of 2 
            n |= n >> 2;
            n |= n >> 4;
            n |= n >> 8;
            n |= n >> 16;

            return MultiplyDeBruijnBitPosition[(uint)(n * 0x07C4ACDDU) >> 27];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int MostSignificantBit(long nn)
        {
            unchecked
            {                
                if (nn == 0) return 0;

                ulong n = (ulong) nn;
                int msb = 0;

                if ((n & 0xFFFFFFFF00000000L) != 0)
                {
                    n >>= (1 << 5);
                    msb += (1 << 5);
                }

                if ((n & 0xFFFF0000) != 0)
                {
                    n >>= (1 << 4);
                    msb += (1 << 4);
                }

                // Now we find the most significant bit in a 16-bit word.

                n |= n << 16;
                n |= n << 32;

                ulong y = n & 0xFF00F0F0CCCCAAAAL;

                ulong t = 0x8000800080008000L & (y | ((y | 0x8000800080008000L) - (n ^ y)));

                t |= t << 15;
                t |= t << 30;
                t |= t << 60;

                return (int)((ulong)msb + (t >> 60));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int MostSignificantBit(ulong n)
        {
            if ( n == 0 ) return 0;
        
            ulong msb = 0;
        
            if ( ( n & 0xFFFFFFFF00000000L ) != 0 ) {
                n >>= ( 1 << 5 );
                msb += ( 1 << 5 );
            }
        
            if ( ( n & 0xFFFF0000 ) != 0 ) {
                n >>= ( 1 << 4 );
                msb += ( 1 << 4 );
            }
        
            // Now we find the most significant bit in a 16-bit word.
        
            n |= n << 16;
            n |= n << 32;
        
            ulong y = n & 0xFF00F0F0CCCCAAAAL;
        
            ulong t = 0x8000800080008000L & ( y | (( y | 0x8000800080008000L ) - ( n ^ y )));
        
            t |= t << 15;
            t |= t << 30;
            t |= t << 60;
        
            return (int)( msb + ( t >> 60 ) );
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int LeadingZeroes(int n)
        {
#if NET6_0_OR_GREATER
            return BitOperations.LeadingZeroCount((uint)n);
#else
            if (n == 0)
                return 32;
            return 31 - MostSignificantBit(n);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int LeadingZeroes(uint n)
        {
#if NET6_0_OR_GREATER
            return BitOperations.LeadingZeroCount(n);
#else
            if (n == 0)
                return 32;
            return 31 - MostSignificantBit(n);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int LeadingZeroes(long n)
        {
#if NET6_0_OR_GREATER
            return BitOperations.LeadingZeroCount((ulong)n);
#else
            if (n == 0)
                return 64;
            return 63 - MostSignificantBit(n);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int LeadingZeroes(ulong n)
        {
#if NET6_0_OR_GREATER
            return BitOperations.LeadingZeroCount(n);
#else
            if (n == 0)
                return 64;
            return 63 - MostSignificantBit(n);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CeilLog2(int n)
        {
            int v = n;
            v |= v >> 1; // first round down to one less than a power of 2 
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;

            int pos = MultiplyDeBruijnBitPosition[(uint)(v * 0x07C4ACDDU) >> 27];
            if (n > (v & ~(v >> 1)))
                return pos + 1;

            return pos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CeilLog2(uint n)
        {
            uint v = n;
            v |= v >> 1; // first round down to one less than a power of 2 
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;

            int pos = MultiplyDeBruijnBitPosition[(uint)(v * 0x07C4ACDDU) >> 27];
            if (n > (v & ~(v >> 1)))
                return pos + 1;

            return pos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FloorLog2(uint n)
        {
#if NET6_0_OR_GREATER
            return BitOperations.Log2(n);
#else

            uint v = n;
            v |= v >> 1; // first round down to one less than a power of 2 
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;

            return MultiplyDeBruijnBitPosition[(uint)(v * 0x07C4ACDDU) >> 27];
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FloorLog2(int n)
        {
#if NET6_0_OR_GREATER
            return BitOperations.Log2((uint)n);
#else
            int v = n;
            v |= v >> 1; // first round down to one less than a power of 2 
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;

            return MultiplyDeBruijnBitPosition[(uint)(v * 0x07C4ACDDU) >> 27];
#endif
        }

        private static readonly int[] powerOf2Table =
        {
              0,   1,   2,   4,   4,   8,   8,   8,   8,  16,  16,  16,  16,  16,  16,  16, 
             16,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,
             32,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64, 
             64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64, 
             64, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 
            128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 
            128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 
            128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 
            128, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 
            256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 
            256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 
            256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 
            256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 
            256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 
            256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 
            256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int PowerOf2(int v)
        {
#if NET6_0_OR_GREATER
            return (int)BitOperations.RoundUpToPowerOf2((uint)v);
#else
            if (v < powerOf2Table.Length)
                return powerOf2Table[v];
            return PowerOf2Internal(v);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long PowerOf2(long v)
        {
#if NET6_0_OR_GREATER
            return (long)BitOperations.RoundUpToPowerOf2((ulong)v);
#else
            if (v < powerOf2Table.Length)
                return powerOf2Table[v];
            return PowerOf2Internal(v);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int PowerOf2Internal(int v)
        {
            if (v > GB)
                ThrowPowerOf2OverflowException(v);

            v--;
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v++;

            return v;
        }

        private static void ThrowPowerOf2OverflowException(int v)
        {
            throw new ArgumentException($"Could not return next power of 2 of {v} because the resulting number exceeds return type int");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long PowerOf2Internal(long v)
        {
            v--;
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            v++;

            return v;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ToBytes(int bits)
        {
            int bytes = Math.DivRem(bits, 8, out var remainder);
            if (remainder > 0)
                bytes++;
            return bytes;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint RotateLeft32(uint value, int count)
        {
            return (value << count) | (value >> (32 - count));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint RotateRight32(uint value, int count)
        {
            return (value >> count) | (value << (32 - count));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong RotateLeft64(ulong value, int count)
        {
            return (value << count) | (value >> (64 - count));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong RotateRight64(ulong value, int count)
        {
            return (value >> count) | (value << (64 - count));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint SwapBytes(uint value)
        {
#if NET6_0_OR_GREATER
            return BinaryPrimitives.ReverseEndianness(value);
#else
            return ((value & 0xff000000) >> 24) |
                   ((value & 0x00ff0000) >> 8)  |
                   ((value & 0x0000ff00) << 8)  |
                   ((value & 0x000000ff) << 24);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int SwapBytes(int value)
        {
            return (int) SwapBytes((uint) value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long SwapBytes(long value)
        {
            return (long)SwapBytes((ulong)value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong SwapBytes(ulong value)
        {
#if NET6_0_OR_GREATER
            return BinaryPrimitives.ReverseEndianness(value);
#else

            return (((value & 0xff00000000000000UL) >> 56) |
                    ((value & 0x00ff000000000000UL) >> 40) |
                    ((value & 0x0000ff0000000000UL) >> 24) |
                    ((value & 0x000000ff00000000UL) >> 8) |
                    ((value & 0x00000000ff000000UL) << 8) |
                    ((value & 0x0000000000ff0000UL) << 24) |
                    ((value & 0x000000000000ff00UL) << 40) |
                    ((value & 0x00000000000000ffUL) << 56));
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TrailingZeroesInBytes(ulong value)
        {
            return DeBruijnBytePos64[((value & (ulong)(-(long)value)) * 0x0218A392CDABBD3FUL) >> 58];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TrailingZeroesInBytes(long value)
        {
            return DeBruijnBytePos64[((ulong)(value & -value) * 0x0218A392CDABBD3FUL) >> 58];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TrailingZeroesInBytes(uint value)
        {
            return DeBruijnBytePos32[((value & (uint)(-(int)value)) * 0x077CB531U) >> 27];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TrailingZeroesInBytes(int value)
        {
            return DeBruijnBytePos32[((uint)(value & -value) * 0x077CB531U) >> 27];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsPowerOfTwo(int value)
        {
            return value != 0 && (value & (value - 1)) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint ByteForBit(int idx)
        {
            return (uint)(idx >> (int)BitVector.Log2BitsPerByte);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte BitInByte(int idx)
        {
            // PERF: Will do the same thing using less bytes.
            //       For reference this is equivalent to [ 0x80 >> (idx % (int)BitsPerByte) ]
            return (byte)(0x80 >> (idx & (BitVector.BitsPerByte - 1)));
        }
        
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static  long DoubleToSortableLong(double val)
        {
            long f = BitConverter.DoubleToInt64Bits(val);
            if (f < 0)
                f ^= 0x7fffffffffffffffL;
            return f;
        }
    }
}
