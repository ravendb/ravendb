using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Bits = Sparrow.Binary.Bits;

namespace Sparrow
{
    public class CaseInsensitiveStringSegmentEqualityComparer : IEqualityComparer<StringSegment>
    {
        public static CaseInsensitiveStringSegmentEqualityComparer Instance = new CaseInsensitiveStringSegmentEqualityComparer();

        [ThreadStatic]
        private static char[] _buffer;


        public bool Equals(StringSegment x, StringSegment y)
        {
            if (x.Length != y.Length)
                return false;
            var compare = string.Compare(x.String, x.Start, y.String, y.Start, x.Length, StringComparison.OrdinalIgnoreCase);
            return compare == 0;
        }

        public unsafe int GetHashCode(StringSegment str)
        {
            if (_buffer == null || _buffer.Length < str.Length)
                _buffer = new char[Bits.NextPowerOf2(str.Length)];

            for (int i = 0; i < str.Length; i++)
            {
                _buffer[i] = char.ToUpperInvariant(str.String[str.Start + i]);
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

    public struct StringSegmentEqualityStructComparer : IEqualityComparer<StringSegment>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(StringSegment x, StringSegment y)
        {
            int xSize = x.Length;
            int ySize = y.Length;
            if (xSize != ySize)
                goto ReturnFalse;  // PERF: Because this method is going to be inlined, in case of false we will want to jump at the end.     

            int xStart = x.Start;
            int yStart = y.Start;
            string xStr = x.String;
            string yStr = y.String;
            for (int i = 0; i < xSize; i++)
            {
                if (xStr[xStart + i] != yStr[yStart + i])
                    goto ReturnFalse;
            }
            return true;

            ReturnFalse: return false;

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(StringSegment x)
        {            
            int xStart = x.Start;
            int xSize = x.Length;
            string xStr = x.String;

            uint hash = 0;
            for (int i = 0; i < xSize; i++)
            {
                hash = Hashing.Combine(hash, xStr[xStart + i]);
            }

            return (int)hash;
        }
    }

    public class StringSegmentEqualityComparer : IEqualityComparer<StringSegment>
    {
        public static StringSegmentEqualityComparer Instance = new StringSegmentEqualityComparer();


        public unsafe bool Equals(StringSegment x, StringSegment y)
        {
            if (x.Length != y.Length)
                return false;

            fixed (char* pX = x.String)
            fixed (char* pY = y.String)
            {
                return Memory.Compare((byte*)pX + x.Start * sizeof(char), (byte*)pY + y.Start * sizeof(char), x.Length * sizeof(char)) == 0;
            }

        }

        public unsafe int GetHashCode(StringSegment str)
        {
            fixed (char* p = str.String)
            {
                //PERF: JIT will remove the corresponding line based on the target architecture using dead code removal.                                 
                if (IntPtr.Size == 4)
                    return (int)Hashing.XXHash32.CalculateInline(((byte*)p + str.Start * sizeof(char)), str.Length * sizeof(char));
                return (int)Hashing.XXHash64.CalculateInline(((byte*)p + str.Start * sizeof(char)), (ulong)str.Length * sizeof(char));
            }
        }
    }

    public struct StringSegment : IEquatable<StringSegment>
    {
        public readonly string String;
        public readonly int Length;
        public readonly int Start;

        private string _valueString;
        public string Value => _valueString ?? (_valueString = String.Substring(Start, Length));


        // PERF: Included this version to exploit the knowledge that we are going to get a full string.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StringSegment(string s)
        {
            String = s;
            Start = 0;
            Length = s.Length;
            _valueString = s;
        }

        // PERF: Included this version to exploit the knowledge that we are going to get a substring starting at 0.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StringSegment(string source, int length)
        {
            String = source;
            Start = 0;
            Length = length;

            int stringLength = source.Length;            

            if (length <= stringLength)
            {
                // PERF: Inverted the condition to ensure the layout of the code will be continuous
                _valueString = length == stringLength ? source : null;
            }
            else
            {
                ThrowIndexOutOfRangeException();
                _valueString = null; // will never reach, this exist to fool the compiler.
            }
        }

        // PERF: Rearranged the parameters to make the other constructors available.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StringSegment(string source, int length, int start)
        {
            String = source;
            Start = start;
            Length = length;

            int stringLength = source.Length;
            if (start + length <= stringLength)
            {
                // PERF: Inverted the condition to ensure the layout of the code will be continuous
                _valueString = start == 0 && length == stringLength ? source : null;
            }
            else
            {
                ThrowIndexOutOfRangeException();
                _valueString = null; // will never reach, this exist to fool the compiler.
            }
        }

        private static void ThrowIndexOutOfRangeException()
        {
            throw new IndexOutOfRangeException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StringSegment SubSegment(int start, int length = -1)
        {
            if (length == -1)
                length = Length - start;
            else if (start + length > String.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            return new StringSegment(String, length, Start + start);
        }

        public char this[int index]
        {
            get
            {
                if (index < 0 || index >= Length)
                    throw new IndexOutOfRangeException();

                return String[Start + index];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator StringSegment(string str)
        {
            return new StringSegment(str);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator string(StringSegment segment)
        {
            return segment.Value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int IndexOfAny(char[] charArray, int startIndex)
        {
            var remainingSegmentLength = Length - startIndex;

            //out of boundary, nothing to check
            if (Start + startIndex >= String.Length ||
                remainingSegmentLength <= 0)
                return -1;

            //zero based index since we are in a segment
            var indexOfAny = String.IndexOfAny(charArray, Start + startIndex, remainingSegmentLength);
            if (indexOfAny == -1)
                return -1;

            return indexOfAny - Start;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int IndexOfLast(char[] charArray)
        {
            var indexOfAny = String.LastIndexOfAny(charArray, Length - 1, Length - Start);
            if (indexOfAny == -1)
                return -1;

            return indexOfAny - Start;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is StringSegment && Equals((StringSegment)obj);
        }

        public override unsafe int GetHashCode()
        {
            fixed (char* p = String)
            {
                //PERF: JIT will remove the corresponding line based on the target architecture using dead code removal.                                 
                if (IntPtr.Size == 4)
                    return (int)Hashing.XXHash32.CalculateInline((byte*)p + Start * sizeof(char), Length * sizeof(char));
                return (int)Hashing.XXHash64.CalculateInline((byte*)p + Start * sizeof(char), (ulong)Length * sizeof(char));
            }
        }

        public unsafe bool Equals(string other)
        {
            if (Length != other.Length)
                return false;

            fixed (char* pSelf = String)
            fixed (char* pOther = other)
            {
                return Memory.Compare((byte*)pSelf + Start * sizeof(char), (byte*)pOther, Length * sizeof(char)) == 0;
            }
        }


        public bool Equals(string other, StringComparison stringComparison)
        {
            if (Length != other.Length)
                return false;
            return string.Compare(String, Start, other, 0, Length, stringComparison) == 0;
        }

        public unsafe bool Equals(StringSegment other)
        {
            if (Length != other.Length)
                return false;

            fixed (char* pSelf = String)
            fixed (char* pOther = other.String)
            {
                return Memory.Compare((byte*)pSelf + Start * sizeof(char), (byte*)pOther + other.Start * sizeof(char), Length * sizeof(char)) == 0;
            }
        }

        public bool Equals(StringSegment other, StringComparison stringComparison)
        {
            if (Length != other.Length)
                return false;
            return string.Compare(String, Start, other.String, other.Start, Length, stringComparison) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override string ToString()
        {
            return Value;
        }


        public bool IsNullOrWhiteSpace()
        {
            if (String == null)
                return true;
            if (Length == 0)
                return true;
            for (int i = 0; i < Length; i++)
            {
                if (char.IsWhiteSpace(String[i + Start]) == false)
                    return false;
            }
            return true;
        }
    }
}