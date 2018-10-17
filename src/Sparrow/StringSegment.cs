using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Utils;
using Bits = Sparrow.Binary.Bits;

namespace Sparrow
{
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
                _buffer = new char[Bits.NextPowerOf2(str.Length)];

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

    public struct StringSegmentEqualityStructComparer : IEqualityComparer<StringSegment>
    {
        public static IEqualityComparer<StringSegment> BoxedInstance = new StringSegmentEqualityComparer();
        
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

            ReturnFalse: return false;

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

    public class StringSegmentEqualityComparer : IEqualityComparer<StringSegment>
    {
        public static StringSegmentEqualityComparer Instance = new StringSegmentEqualityComparer();


        public unsafe bool Equals(StringSegment x, StringSegment y)
        {
            if (x.Length != y.Length)
                return false;

            fixed (char* pX = x.Buffer)
            fixed (char* pY = y.Buffer)
            {
                return Memory.Compare((byte*)pX + x.Offset * sizeof(char), (byte*)pY + y.Offset * sizeof(char), x.Length * sizeof(char)) == 0;
            }

        }

        public unsafe int GetHashCode(StringSegment str)
        {
            fixed (char* p = str.Buffer)
            {
                //PERF: JIT will remove the corresponding line based on the target architecture using dead code removal.                                 
                if (IntPtr.Size == 4)
                    return (int)Hashing.XXHash32.CalculateInline(((byte*)p + str.Offset * sizeof(char)), str.Length * sizeof(char));
                return (int)Hashing.XXHash64.CalculateInline(((byte*)p + str.Offset * sizeof(char)), (ulong)str.Length * sizeof(char));
            }
        }
    }

    public struct StringSegment : IEquatable<StringSegment>
    {
        public readonly string Buffer;
        public readonly int Length;
        public readonly int Offset;

        private string _valueString;
        public string Value => _valueString ?? (_valueString = Buffer?.Substring(Offset, Length));


        // PERF: Included this version to exploit the knowledge that we are going to get a full string.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StringSegment(string buffer)
        {
            Offset = 0;
            Length = buffer?.Length ?? 0;
            Buffer = buffer;
            _valueString = buffer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StringSegment(string buffer, int offset, int length)
        {
            Debug.Assert(buffer != null);
            Debug.Assert(offset >= 0);
            Debug.Assert(length >= 0);
            Debug.Assert(offset + length <= buffer.Length);

            Offset = offset;
            Length = length;
            Buffer = buffer;
            _valueString = null;
            if (Offset == 0 && Length == buffer.Length)
                _valueString = buffer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StringSegment Subsegment(int offset, int length = -1)
        {
            Debug.Assert(offset >= 0 && offset <= Length);
            if (length == -1)
                length = Length - offset;
            Debug.Assert(length >= 0 && offset + length <= Length);
            return new StringSegment(Buffer, Offset + offset, length);
        }

        // String's indexing will throw a IndexOutOfRange exception if required
        public char this[int index]
        {
            get
            {
                Debug.Assert(index >= 0 && index < Length);
                return Buffer[Offset + index];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator StringSegment(string buffer)
        {
            return new StringSegment(buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator string(StringSegment segment)
        {
            return segment.Value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool StartsWith(string prefix)
        {
            if (prefix.Length > Length)
                return false;
            return string.CompareOrdinal(Buffer, Offset, prefix, 0, prefix.Length) == 0;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool EndsWith(string postfix)
        {
            if (postfix.Length > Length)
                return false;
            return string.CompareOrdinal(Buffer, Offset + Length - postfix.Length, postfix, 0, postfix.Length) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int IndexOfAny(char[] charArray, int startIndex)
        {
            if (startIndex < 0 && startIndex >= Length)
                ThrowOutOfRangeIndex(startIndex);
            //zero based index since we are in a segment
            var indexOfAny = Buffer.IndexOfAny(charArray, Offset + startIndex, Length - startIndex);
            if (indexOfAny == -1)
                return -1;

            return indexOfAny - Offset;
        }

        private void ThrowOutOfRangeIndex(int startIndex)
        {
            throw new ArgumentOutOfRangeException(nameof(startIndex), $"startIndex has value of {startIndex} but the length of \'{this}\' is {Length}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int IndexOfLast(char[] charArray)
        {
            var indexOfAny = Buffer.LastIndexOfAny(charArray, Offset + Length - 1, Length);
            if (indexOfAny == -1)
                return -1;

            return indexOfAny - Offset;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is StringSegment && Equals((StringSegment)obj);
        }

        public override unsafe int GetHashCode()
        {
            fixed (char* p = Buffer)
            {
                //PERF: JIT will remove the corresponding line based on the target architecture using dead code removal.                                 
                if (IntPtr.Size == 4)
                    return (int)Hashing.XXHash32.CalculateInline((byte*)p + Offset * sizeof(char), Length * sizeof(char));
                return (int)Hashing.XXHash64.CalculateInline((byte*)p + Offset * sizeof(char), (ulong)Length * sizeof(char));
            }
        }

        public unsafe bool Equals(string other)
        {
            if (other == null)
                return Buffer == null;

            if (Length != other.Length)
                return false;

            fixed (char* pSelf = Buffer)
            fixed (char* pOther = other)
            {
                return Memory.Compare((byte*)pSelf + Offset * sizeof(char), (byte*)pOther, Length * sizeof(char)) == 0;
            }
        }


        public bool Equals(string other, StringComparison stringComparison)
        {
            if (other == null)
                return Buffer == null;

            if (Length != other.Length)
                return false;
            return string.Compare(Buffer, Offset, other, 0, Length, stringComparison) == 0;
        }



        public int Compare(string other, StringComparison stringComparison)
        {
            if (other == null)
                return 1;

            var result = string.Compare(Buffer, Offset, other, 0, Length, stringComparison);
            if (result == 0)
                return Length - other.Length;
            return result;
        }


        public int Compare(StringSegment other, StringComparison stringComparison)
        {
            var result = string.Compare(Buffer, Offset, other.Buffer, other.Offset, Length, stringComparison);
            if (result == 0)
                return Length - other.Length;
            return result;
        }

        public unsafe bool Equals(StringSegment other)
        {
            if (Length != other.Length)
                return false;

            fixed (char* pSelf = Buffer)
            fixed (char* pOther = other.Buffer)
            {
                return Memory.Compare((byte*)pSelf + Offset * sizeof(char), (byte*)pOther + other.Offset * sizeof(char), Length * sizeof(char)) == 0;
            }
        }

        public bool Equals(StringSegment other, StringComparison stringComparison)
        {
            if (Length != other.Length)
                return false;
            return string.Compare(Buffer, Offset, other.Buffer, other.Offset, Length, stringComparison) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override string ToString()
        {
            return Value;
        }


        public bool IsNullOrWhiteSpace()
        {
            if (Buffer == null || Length == 0)
                return true;
            for (int i = 0; i < Length; i++)
            {
                if (char.IsWhiteSpace(Buffer[Offset + i]) == false)
                    return false;
            }
            return true;
        }
    }
}
