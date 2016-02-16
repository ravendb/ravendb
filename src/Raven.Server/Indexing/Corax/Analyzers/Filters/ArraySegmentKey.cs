using System;
using System.Text;

namespace Raven.Server.Indexing.Corax.Analyzers.Filters
{
    public struct ArraySegmentKey<T>
        where T : IEquatable<T>
    {
        private readonly T[] _buffer;
        private readonly int _size;

        public ArraySegmentKey(T[] buffer)
            : this(buffer, buffer.Length)
        {
        }

        public ArraySegmentKey(T[] buffer, int size)
            : this()
        {
            _buffer = buffer;
            _size = size;
        }

        public int Size
        {
            get { return _size; }
        }

        public T[] Buffer
        {
            get { return _buffer; }
        }

        private bool Equals(ArraySegmentKey<T> other)
        {
            if (_buffer == null || other._buffer == null)
                return false;
            if (_size != other._size)
            {
                return false;
            }

            for (int i = 0; i < _size; i++)
            {
                if (_buffer[i].Equals(other._buffer[i]) == false)
                    return false;
            }
            return true;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is ArraySegmentKey<T> && Equals((ArraySegmentKey<T>)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                if (_buffer == null)
                    return -1;
                int hc = _size;
                for (int i = 0; i < _size; i++)
                {
                    hc = _buffer[i].GetHashCode() * 397 ^ hc;
                }
                return hc;
            }
        }

        public override string ToString()
        {
            var c = _buffer as char[];
            if (c != null)
                return new string(c, 0, Size);
            var b = _buffer as byte[];
            if (b != null)
                return Encoding.UTF8.GetString(b, 0, Size);

            return base.ToString();
        }
    }
}