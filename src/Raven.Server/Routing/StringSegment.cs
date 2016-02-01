using System;
using System.Diagnostics.CodeAnalysis;

namespace Raven.Server.Routing
{
    public struct StringSegment : IEquatable<StringSegment>
    {
        string _string;
        int _start;
        int _count;

        public StringSegment(string s, int start, int count)
        {
            _string = s;
            _start = start;
            _count = count;
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
                for (int i = 0; i < _count; i++)
                {
                    hashCode = (hashCode * 397) ^ char.ToLowerInvariant(_string[_start + i]);

                }
                return hashCode;
            }
        }

        public bool Equals(string other)
        {
            if (_count != other.Length)
                return false;
            return string.Compare(_string, _start, other, 0, _count, StringComparison.OrdinalIgnoreCase) == 0;
        }

        public bool Equals(StringSegment other)
        {
            if (_count != other._count)
                return false;
            return string.Compare(_string, _start, other._string, other._start, _count, StringComparison.OrdinalIgnoreCase) == 0;
        }

        public override string ToString()
        {
            _string =  _string.Substring(_start, _count);
            _start = 0;
            return _string;
        }


        public bool IsNullOrWhiteSpace()
        {
            if (_string == null)
                return true;
            if (_count == 0)
                return true;
            for (int i = 0; i < _count; i++)
            {
                if (char.IsWhiteSpace(_string[i + _start]) == false)
                    return false;
            }
            return true;
        }
    }
}