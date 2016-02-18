using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Raven.Server.Utils
{
    public struct StringSegment : IEquatable<StringSegment>
    {
        private readonly string _string;

        public int Length { get; }
        public int Start { get; }

        private string _valueString;
        public string Value => _valueString ?? (_valueString = _string.Substring(Start, Length));

        public StringSegment(string s, int start, int count = -1)
        {
            _string = s;
            Start = start;
            Length = count == -1 ? _string.Length - start : count;
            _valueString = null;			

            if (Start + Length > _string.Length)
                throw new IndexOutOfRangeException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StringSegment SubSegment(int start, int length = -1)
        {
            if (length == -1)
                length = Length - start;
            else if (start + length > _string.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            return new StringSegment(_string,Start + start,length);
        }

        public char this[int index]
        {
            get
            {
                if (index < 0 || index >= Length)
                    throw new IndexOutOfRangeException();

                return _string[Start + index];
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
            if (Start + startIndex >= _string.Length ||
                remainingSegmentLength <= 0)
                return -1;

            //zero based index since we are in a segment
            var indexOfAny = _string.IndexOfAny(charArray, Start + startIndex,remainingSegmentLength);
            if (indexOfAny == -1)
                return -1;

            return indexOfAny - Start;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is StringSegment && Equals((StringSegment)obj);
        }

        [SuppressMessage("ReSharper", "NonReadonlyMemberInGetHashCode")]
        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = 0;
                for (int i = 0; i < Length; i++)
                {
                    hashCode = (hashCode * 397) ^ char.ToLowerInvariant(_string[Start + i]);

                }
                return hashCode;
            }
        }

        public bool Equals(string other)
        {
            if (Length != other.Length)
                return false;
            return string.Compare(_string, Start, other, 0, Length, StringComparison.OrdinalIgnoreCase) == 0;
        }

        public bool Equals(StringSegment other)
        {
            if (Length != other.Length)
                return false;
            return string.Compare(_string, Start, other._string, other.Start, Length, StringComparison.OrdinalIgnoreCase) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override string ToString()
        {
            return Value;
        }


        public bool IsNullOrWhiteSpace()
        {
            if (_string == null)
                return true;
            if (Length == 0)
                return true;
            for (int i = 0; i < Length; i++)
            {
                if (char.IsWhiteSpace(_string[i + Start]) == false)
                    return false;
            }
            return true;
        }
    }
}