using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Sparrow
{
    /// <summary>
    /// The struct compared is optimized for small strings that come from the outside of the controlled fence, 
    /// the hash function used is compact (making it suitable for inlining) and also flood resistant. 
    /// </summary>
    public struct OrdinalStringStructComparer : IEqualityComparer<string>
    {
        public static readonly OrdinalStringStructComparer Instance = default(OrdinalStringStructComparer);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(string x, string y)
        {
            int xSize = x.Length;
            int ySize = y.Length;
            if (xSize != ySize)
                goto ReturnFalse;  // PERF: Because this method is going to be inlined, in case of false we will want to jump at the end.     

            for (int i = 0; i < xSize; i++)
            {
                if (x[i] != y[i])
                    goto ReturnFalse;
            }

            return true;

            ReturnFalse: return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(string x)
        {
            return (int)Hashing.Marvin32.CalculateInline<Hashing.OrdinalModifier>(x);
        }
    }

    /// <summary>
    /// The struct compared is optimized for small strings that come from the outside of the controlled fence, 
    /// the hash function used is compact (making it suitable for inlining) and also flood resistant. 
    /// </summary>
    public struct OrdinalIgnoreCaseStringStructComparer : IEqualityComparer<string>
    {        
        public static readonly OrdinalIgnoreCaseStringStructComparer Instance = default(OrdinalIgnoreCaseStringStructComparer);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(string x, string y)
        {
            int xSize = x.Length;
            int ySize = y.Length;
            if (xSize != ySize)
                goto ReturnFalse;  // PERF: Because this method is going to be inlined, in case of false we will want to jump at the end.     

            for (int i = 0; i < xSize; i++)
            {
                char xch = x[i];
                char ych = y[i];

                if (xch >= 65 && xch <= 90)
                    xch = (char)(xch | 0x0020);
                if (ych >= 65 && ych <= 90)
                    ych = (char)(ych | 0x0020);

                if (xch != ych)
                    goto ReturnFalse;
            }

            return true;

            ReturnFalse: return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(string x)
        {
            return (int)Hashing.Marvin32.CalculateInline<Hashing.OrdinalIgnoreCaseModifier>(x);
        }
    }
}
