using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Binary
{
    public static class Bits
    {

        // Code taken from http://graphics.stanford.edu/~seander/bithacks.html#IntegerLogDeBruijn

        private static readonly int[] MultiplyDeBruijnBitPosition = new int[]
                {
                    0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30,
                    8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31
                };


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int MostSignificantBit(uint n)
        {
            n |= n >> 1; // first round down to one less than a power of 2 
            n |= n >> 2;
            n |= n >> 4;
            n |= n >> 8;
            n |= n >> 16;

            return MultiplyDeBruijnBitPosition[(uint)(n * 0x07C4ACDDU) >> 27];
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
        public static int LeadingZeroes(int n)
        {
            if (n == 0)
                return 32;
            return 31 - MostSignificantBit(n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int LeadingZeroes(uint n)
        {
            if (n == 0)
                return 32;
            return 31 - MostSignificantBit(n);
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
            else
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
            else
                return pos;
        }

    }
}
