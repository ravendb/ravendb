using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Sparrow.Binary;
using Sparrow.Utils;

namespace Sparrow
{

    public struct StringSegmentEqualityStructComparer : IEqualityComparer<StringSegment>
    {
        public static IEqualityComparer<StringSegment> BoxedInstance = new StringSegmentEqualityStructComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(StringSegment x, StringSegment y)
        {
            int xSize = x.Length;
            int ySize = y.Length;
            if (xSize != ySize)
                goto ReturnFalse;  // PERF: Because this method is going to be inlined, in case of false we will want to jump at the end.     

            int xStart = x.Offset;
            int yStart = y.Offset;
            string xStr = x.Buffer;
            string yStr = y.Buffer;
            for (int i = 0; i < xSize; i++)
            {
                if (xStr[xStart + i] != yStr[yStart + i])
                    goto ReturnFalse;
            }
            return true;

        ReturnFalse:
            return false;

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(StringSegment x)
        {
            int xStart = x.Offset;
            int xSize = x.Length;
            string xStr = x.Buffer;

            uint hash = 0;
            for (int i = 0; i < xSize; i++)
            {
                hash = Hashing.Combine(hash, xStr[xStart + i]);
            }

            return (int)hash;
        }
    }

    public class CaseInsensitiveStringSegmentEqualityComparer : IEqualityComparer<StringSegment>
    {
        public static CaseInsensitiveStringSegmentEqualityComparer Instance = new CaseInsensitiveStringSegmentEqualityComparer();

        [ThreadStatic]
        private static char[] _buffer;

        static CaseInsensitiveStringSegmentEqualityComparer()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () => _buffer = null;
        }

        public bool Equals(StringSegment x, StringSegment y)
        {
            if (x.Length != y.Length)
                return false;
            var compare = string.Compare(x.Buffer, x.Offset, y.Buffer, y.Offset, x.Length, StringComparison.OrdinalIgnoreCase);
            return compare == 0;
        }

        public unsafe int GetHashCode(StringSegment str)
        {
            if (_buffer == null || _buffer.Length < str.Length)
                _buffer = new char[Bits.PowerOf2(str.Length)];

            for (int i = 0; i < str.Length; i++)
            {
                _buffer[i] = char.ToUpperInvariant(str.Buffer[str.Offset + i]);
            }

            fixed (char* p = _buffer)
            {
                //PERF: JIT will remove the corresponding line based on the target architecture using dead code removal.                                 
                if (IntPtr.Size == 4)
                    return (int)Hashing.XXHash32.CalculateInline((byte*)p, str.Length * sizeof(char));
                return (int)Hashing.XXHash64.CalculateInline((byte*)p, (ulong)str.Length * sizeof(char));
            }
        }
    }

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
