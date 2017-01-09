using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Binary;

namespace Sparrow.Json
{
    public class LazyStringValueComparer : IEqualityComparer<LazyStringValue>
    {
        public static readonly LazyStringValueComparer Instance = new LazyStringValueComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(LazyStringValue x, LazyStringValue y)
        {
            if (x == y) return true;
            if (x == null || y == null) return false;
            return x.CompareTo(y) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(LazyStringValue obj)
        {
            unsafe
            {
                return (int)Hashing.XXHash32.CalculateInline(obj.Buffer, obj.Size);
            }            
        }
    }

    public unsafe class LazyStringValue : IComparable<string>, IEquatable<string>,
        IComparable<LazyStringValue>, IEquatable<LazyStringValue>, IDisposable, IComparable
    {
        private readonly JsonOperationContext _context;
        private string _string;

        private byte* _buffer;
        public byte this[int index] => Buffer[index];
        public byte* Buffer => _buffer;

        private readonly int _size;
        public int Size => _size;

        private int _length = -1;
        public int Length
        {
            get
            {
                // Lazily load the length from the buffer. This is an O(n)
                if (_length == -1 && Buffer != null)
                    _length = _context.Encoding.GetCharCount(Buffer, Size);
                return _length;
            }
        }

        [ThreadStatic]
        private static char[] LazyStringTempBuffer;

        [ThreadStatic]
        private static byte[] LazyStringTempComparisonBuffer;

        public int[] EscapePositions;
        public AllocatedMemoryData AllocatedMemoryData;
        public int? LastFoundAt;

        public LazyStringValue(string str, byte* buffer, int size, JsonOperationContext context)
        {
            Debug.Assert(size >= 0);
            Debug.Assert(context != null);
            _size = size;
            _context = context;
            _buffer = buffer;
            _string = str;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(string other)
        {
            return CompareTo(other) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(LazyStringValue other)
        {
            return CompareTo(other) == 0;
        }

        public int CompareTo(string other)
        {
            ThrowIfDisposed();
            if (_string != null)
                return String.Compare(_string, other, StringComparison.Ordinal);

            var sizeInBytes = _context.Encoding.GetMaxByteCount(other.Length);

            if (LazyStringTempComparisonBuffer == null || LazyStringTempComparisonBuffer.Length < other.Length)
                LazyStringTempComparisonBuffer = new byte[Bits.NextPowerOf2(sizeInBytes)];

            fixed (char* pOther = other)
            fixed (byte* pBuffer = LazyStringTempComparisonBuffer)
            {
                var tmpSize = _context.Encoding.GetBytes(pOther, other.Length, pBuffer, sizeInBytes);
                return Compare(pBuffer, tmpSize);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareTo(LazyStringValue other)
        {
            ThrowIfDisposed();
            if (other.Buffer == Buffer && other.Size == Size)
                return 0;
            return Compare(other.Buffer, other.Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(byte* other, int otherSize)
        {
            ThrowIfDisposed();
            var result = Memory.CompareInline(Buffer, other, Math.Min(Size, otherSize));
            return result == 0 ? Size - otherSize : result;
        }

        public static bool operator ==(LazyStringValue self, LazyStringValue str)
        {
            self?.ThrowIfDisposed();
            if (ReferenceEquals(self, null) && ReferenceEquals(str,null))
                return true;
            if (ReferenceEquals(self, null) || ReferenceEquals(str,null))
                return false;
            return self.Equals(str);
        }

        public static bool operator !=(LazyStringValue self, LazyStringValue str)
        {
            self?.ThrowIfDisposed();
            return !(self == str);
        }

        public static bool operator ==(LazyStringValue self, string str)
        {
            self?.ThrowIfDisposed();
            if (ReferenceEquals(self, null) && str == null)
                return true;
            if (ReferenceEquals(self, null) || str == null)
                return false;
            return self.Equals(str);
        }

        public static bool operator !=(LazyStringValue self, string str)
        {
            self?.ThrowIfDisposed();
            return !(self == str);
        }

        public static implicit operator string(LazyStringValue self)
        {            
            if (self == null)
                return null;

            self.ThrowIfDisposed();

            return self._string ?? 
                (self._string = self._context.Encoding.GetString(self._buffer, self._size));
        }

        public override bool Equals(object obj)
        {
            ThrowIfDisposed();

            if (ReferenceEquals(obj, null))
                return false;

            var s = obj as string;
            if (s != null)
                return Equals(s);
            var comparer = obj as LazyStringValue;
            if (comparer != (LazyStringValue)null)
                return Equals(comparer);

            return ReferenceEquals(obj, this);
        }

        public override int GetHashCode()
        {
            ThrowIfDisposed();
            return (int)Hashing.XXHash32.CalculateInline(Buffer, Size);
        }
        
        public override string ToString()
        {
            return (string)this; // invoke the implicit string conversion
        }

        public int CompareTo(object obj)
        {
            ThrowIfDisposed();

            if (obj == null)
                return 1;

            var lsv = obj as LazyStringValue;

            if (lsv != (LazyStringValue)null)
                return CompareTo(lsv);

            var s = obj as string;

            if (s != null)
                return CompareTo(s);

            throw new NotSupportedException($"Cannot compare LazyStringValue to object of type {obj.GetType().Name}");
        }

        public bool IsDisposed => AllocatedMemoryData == null && _buffer == null;        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ThrowIfDisposed()
        {
            if(IsDisposed)
                throw new ObjectDisposedException(nameof(LazyStringValue));
        }

        public void Dispose()
        {
            if (AllocatedMemoryData != null)
            {
                _context.ReturnMemory(AllocatedMemoryData);
                AllocatedMemoryData = null;
                _buffer = null;
                _string = null;
            }
            else if (_buffer != null)
            {
                _buffer = null;
                _string = null;
            }
        }

        public bool Contains(string value)
        {
            ThrowIfDisposed();
            if (_string != null)
                return _string.Contains(value);

            return ToString().Contains(value);
        }

        public bool EndsWith(string value)
        {
            ThrowIfDisposed();
            if (_string != null)
                return _string.EndsWith(value);

            if (value == null)
                throw new ArgumentNullException(nameof(value));
            // Every UTF8 character uses at least 1 byte
            if (value.Length > Size)
                return false;
            if (value.Length == 0)
                return true;

            // We are assuming these values are going to be relatively constant throughout the object lifespan
            LazyStringValue converted = _context.GetLazyStringForFieldWithCaching(value);
            return EndsWith(converted);
        }

        public bool EndsWith(LazyStringValue value)
        {
            ThrowIfDisposed();
            if (value.Size > Size)
                return false;

            return Memory.Compare(Buffer + (Size - value.Size), value.Buffer, value.Size) == 0;
        }

        public bool EndsWith(string value, StringComparison comparisonType)
        {
            return ToString().EndsWith(value, comparisonType);
        }

        public int IndexOf(char value)
        {
            return IndexOf(value, 0, Length);
        }

        public int IndexOf(char value, int startIndex)
        {
            return IndexOf(value, startIndex, Length - startIndex);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ValidateIndexes(int startIndex, int count)
        {
            if (startIndex < 0 || count < 0)
                throw new ArgumentOutOfRangeException("count or startIndex is negative.");

            if (startIndex > Length)
                throw new ArgumentOutOfRangeException(nameof(startIndex), "startIndex is greater than the length of this string.");

            if (count > Length - startIndex)
                throw new ArgumentOutOfRangeException("count is greater than the length of this string minus startIndex.");
        }

        public int IndexOf(char value, int startIndex, int count)
        {
            ThrowIfDisposed();
            if (_string != null)
                return _string.IndexOf(value, startIndex, count);

            ValidateIndexes(startIndex, count);

            if (LazyStringTempBuffer == null || LazyStringTempBuffer.Length < Length)
                LazyStringTempBuffer = new char[Bits.NextPowerOf2(Length)];

            fixed (char* pChars = LazyStringTempBuffer)
                _context.Encoding.GetChars(Buffer, Size, pChars, Length);

            for (int i = startIndex; i < startIndex + count; i++)
            {
                if (LazyStringTempBuffer[i] == value)
                    return i;
            }

            return -1;
        }

        public int IndexOf(string value)
        {
            return ToString().IndexOf(value, StringComparison.Ordinal);
        }

        public int IndexOf(string value, StringComparison comparisonType)
        {
            return ToString().IndexOf(value, comparisonType);
        }

        public int IndexOfAny(char[] anyOf)
        {
            return IndexOfAny(anyOf, 0, Length);
        }

        public int IndexOfAny(char[] anyOf, int startIndex)
        {
            return IndexOfAny(anyOf, startIndex, Length - startIndex);
        }

        public int IndexOfAny(char[] anyOf, int startIndex, int count)
        {
            ThrowIfDisposed();
            if (_string != null)
                return _string.IndexOfAny(anyOf, startIndex, count);

            ValidateIndexes(startIndex, count);

            if (LazyStringTempBuffer == null || LazyStringTempBuffer.Length < Length)
                LazyStringTempBuffer = new char[Bits.NextPowerOf2(Length)];

            fixed (char* pChars = LazyStringTempBuffer)
                _context.Encoding.GetChars(Buffer, Size, pChars, Length);

            for (int i = startIndex; i < startIndex + count; i++)
            {
                if (anyOf.Contains(LazyStringTempBuffer[i]))
                    return i;
            }

            return -1;
        }

        public string Insert(int startIndex, string value)
        {
            return ToString().Insert(startIndex, value);
        }

        public int LastIndexOf(char value)
        {
            return LastIndexOf(value, Length, Length);
        }

        public int LastIndexOf(char value, int startIndex)
        {
            return LastIndexOf(value, startIndex, startIndex);
        }

        public int LastIndexOf(char value, int startIndex, int count)
        {
            ThrowIfDisposed();
            if (_string != null)
                return _string.LastIndexOf(value, startIndex, count);

            ValidateIndexes(Length - startIndex, count);

            if (LazyStringTempBuffer == null || LazyStringTempBuffer.Length < Length)
                LazyStringTempBuffer = new char[Bits.NextPowerOf2(Length)];

            fixed (char* pChars = LazyStringTempBuffer)
                _context.Encoding.GetChars(Buffer, Size, pChars, Length);

            for (int i = startIndex; i > startIndex - count; i++)
            {
                if (LazyStringTempBuffer[i] == value)
                    return i;
            }

            return -1;
        }

        public int LastIndexOf(string value)
        {
            if (_string != null)
                return _string.LastIndexOf(value, StringComparison.Ordinal);

            return ToString().LastIndexOf(value, StringComparison.Ordinal);
        }

        public int LastIndexOf(string value, StringComparison comparisonType)
        {
            return ToString().LastIndexOf(value, comparisonType);
        }

        public int LastIndexOfAny(char[] anyOf)
        {
            return LastIndexOfAny(anyOf, Length, Length);
        }

        public int LastIndexOfAny(char[] anyOf, int startIndex)
        {
            return LastIndexOfAny(anyOf, startIndex, startIndex);
        }

        public int LastIndexOfAny(char[] anyOf, int startIndex, int count)
        {
            ThrowIfDisposed();
            if (_string != null)
                return _string.LastIndexOfAny(anyOf, startIndex, count);

            ValidateIndexes(Length - startIndex, count);

            if (LazyStringTempBuffer == null || LazyStringTempBuffer.Length < Length)
                LazyStringTempBuffer = new char[Bits.NextPowerOf2(Length)];

            fixed (char* pChars = LazyStringTempBuffer)
                _context.Encoding.GetChars(Buffer, Size, pChars, Length);

            for (int i = startIndex; i > startIndex - count; i++)
            {
                if (anyOf.Contains(LazyStringTempBuffer[i]))
                    return i;
            }

            return -1;
        }

        public string PadLeft(int totalWidth)
        {
            return ToString().PadLeft(totalWidth);
        }

        public string PadLeft(int totalWidth, char paddingChar)
        {
            return ToString().PadLeft(totalWidth, paddingChar);
        }

        public string PadRight(int totalWidth)
        {
            return ToString().PadRight(totalWidth);
        }

        public string PadRight(int totalWidth, char paddingChar)
        {
            return ToString().PadRight(totalWidth, paddingChar);
        }

        public string Remove(int startIndex)
        {
            return ToString().Remove(startIndex);
        }

        public string Remove(int startIndex, int count)
        {
            return ToString().Remove(startIndex, count);
        }

        public string Replace(char oldChar, char newChar)
        {
            return ToString().Replace(oldChar, newChar);
        }

        public string Replace(string oldValue, string newValue)
        {
            return ToString().Replace(oldValue, newValue);
        }

        public string Substring(int startIndex)
        {
            return ToString().Substring(startIndex);
        }

        public string Substring(int startIndex, int length)
        {
            return ToString().Substring(startIndex, length);
        }
        public string[] Split(params char[] separator)
        {
            return ToString().Split(separator);
        }

        public string[] Split(char[] separator, StringSplitOptions options)
        {
            return ToString().Split(separator, options);
        }

        public string[] Split(char[] separator, int count)
        {
            return ToString().Split(separator, count);
        }

        public string[] Split(char[] separator, int count, StringSplitOptions options)
        {
            return ToString().Split(separator, count, options);
        }

        public string[] Split(string[] separator, StringSplitOptions options)
        {
            return ToString().Split(separator, options);
        }

        public bool StartsWith(string value)
        {
            ThrowIfDisposed();
            if (_string != null)
                return _string.StartsWith(value);

            if (value == null)
                throw new ArgumentNullException(nameof(value));
            // Every UTF8 character uses at least 1 byte
            if (value.Length > Size)
                return false;
            if (value.Length == 0)
                return true;

            // We are assuming these values are going to be relatively constant throughout the object lifespan
            LazyStringValue converted = _context.GetLazyStringForFieldWithCaching(value);
            return StartsWith(converted);
        }

        public bool StartsWith(LazyStringValue value)
        {
            ThrowIfDisposed();
            if (value.Size > Size)
                return false;

            return Memory.Compare(Buffer, value.Buffer, value.Size) == 0;
        }

        public char[] ToCharArray()
        {
            return ToString().ToCharArray();
        }

        public char[] ToCharArray(int startIndex, int length)
        {
            return ToString().ToCharArray(startIndex, length);
        }

        public string ToLower()
        {
            return ToString().ToLower();
        }

        public string ToLowerInvariant()
        {
            return ToString().ToLowerInvariant();
        }

        public string ToUpper()
        {
            return ToString().ToUpper();
        }

        public string ToUpperInvariant()
        {
            return ToString().ToUpperInvariant();
        }

        public string Trim()
        {
            return ToString().Trim();
        }

        public string Trim(params char[] trimChars)
        {
            return ToString().Trim(trimChars);
        }

        public string TrimEnd()
        {
            return ToString().TrimEnd();
        }

        public string TrimEnd(params char[] trimChars)
        {
            return ToString().TrimEnd(trimChars);
        }

        public string TrimStart()
        {
            return ToString().TrimStart();
        }

        public string TrimStart(params char[] trimChars)
        {
            return ToString().TrimStart(trimChars);
        }

        public string Reverse()
        {
            ThrowIfDisposed();
            var maxCharCount = _context.Encoding.GetMaxCharCount(Length);
            if(LazyStringTempBuffer == null || LazyStringTempBuffer.Length < maxCharCount)
                LazyStringTempBuffer = new char[Bits.NextPowerOf2(maxCharCount)];

            fixed (char* pChars = LazyStringTempBuffer)
            {
                var chars = _context.Encoding.GetChars(_buffer, Length, pChars, LazyStringTempBuffer.Length);
                Array.Reverse(LazyStringTempBuffer, 0, chars);
                return new string(LazyStringTempBuffer, 0, chars);
            }    
        }
    }
}