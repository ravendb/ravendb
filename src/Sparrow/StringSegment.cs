using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Bits = Sparrow.Binary.Bits;

namespace Sparrow
{
    public class CaseInsensitiveStringSegmentEqualityComparer: IEqualityComparer<StringSegment>
    {
        public static CaseInsensitiveStringSegmentEqualityComparer Instance = new CaseInsensitiveStringSegmentEqualityComparer();
        [ThreadStatic] private static char[] _buffer;


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
                return (int)Hashing.XXHash32.CalculateInline((byte*) p, str.Length*sizeof (char));
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

        public StringSegment(string s, int start, int count = -1)
        {
            String = s;
            Start = start;
            Length = count == -1 ? String.Length - start : count;
            _valueString = null;			

            if (Start + Length > String.Length)
                throw new IndexOutOfRangeException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StringSegment SubSegment(int start, int length = -1)
        {
            if (length == -1)
                length = Length - start;
            else if (start + length > String.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            return new StringSegment(String,Start + start,length);
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
            return new StringSegment(str,0);
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
            var indexOfAny = String.IndexOfAny(charArray, Start + startIndex,remainingSegmentLength);
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
                return (int)Hashing.XXHash32.CalculateInline((byte*) p + (Start*sizeof (char)), Length * sizeof (char));
            }
        }

        public bool Equals(string other)
        {
           return Equals(other, StringComparison.Ordinal);
        }


        public bool Equals(string other, StringComparison stringComparison)
        {
            if (Length != other.Length)
                return false;
            return string.Compare(String, Start, other, 0, Length, stringComparison) == 0;
        }

        public bool Equals(StringSegment other)
        {
            return Equals(other, StringComparison.Ordinal);
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