using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Binary
{
    public static class Bits
    {
        
        public static uint MostSignificantBit(uint n)
        {
            n |= (n >> 1);
            n |= (n >> 2);
            n |= (n >> 4);
            n |= (n >> 8);
            n |= (n >> 16);

            return n - (n >> 1);
        }

        public static int MostSignificantBit(int n)
        {
            n |= (n >> 1);
            n |= (n >> 2);
            n |= (n >> 4);
            n |= (n >> 8);
            n |= (n >> 16);

            return n - (n >> 1);
        }
    }
}
