using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Binary;
using Sparrow.Utils;

namespace Sparrow.Json
{
    internal sealed class LazyStringValueComparer : IEqualityComparer<LazyStringValue>
    {
        public static readonly LazyStringValueComparer Instance = new LazyStringValueComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(LazyStringValue x, LazyStringValue y)
        {
            if (x == y)
                return true;
            if (x == null || y == null)
                return false;
            return x.Equals(y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(LazyStringValue obj)
        {
            return obj.GetHashCode();
        }
    }

    internal struct LazyStringValueStructComparer : IEqualityComparer<LazyStringValue>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(LazyStringValue x, LazyStringValue y)
        {
            if (x == y)
                return true;
            if (x == null || y == null)
                return false;
            return x.CompareTo(y) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(LazyStringValue obj)
        {
            unsafe
            {
                //PERF: JIT will remove the corresponding line based on the target architecture using dead code removal.
                if (IntPtr.Size == 4)
                    return (int)Hashing.XXHash32.CalculateInline(obj.Buffer, obj.Size);
                return (int)Hashing.XXHash64.CalculateInline(obj.Buffer, (ulong)obj.Size);
            }
        }
    }

    // PERF: Sealed because in CoreCLR 2.0 it will devirtualize virtual calls methods like GetHashCode.
    public sealed unsafe class LazyStringValue : IComparable<string>, IEquatable<string>,
        IComparable<LazyStringValue>, IEquatable<LazyStringValue>, IDisposable, IComparable, IConvertible, IEnumerable<char>, IDisposableQueryable
    {
        internal JsonOperationContext _context;
        private string _string;

        private string MaterializeStringValue => ToString();

        private byte* _buffer;
        public byte this[int index] => Buffer[index];
        public byte* Buffer => _buffer;

        private int _size;
        public int Size => _size;

        private int _length;

        public int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                // Lazily load the length from the buffer. This is an O(n)
                if (_length == -1 && Buffer != null)
                    _length = Encodings.Utf8.GetCharCount(Buffer, Size);
                return _length;
            }
        }

        public bool EqualsOrdinalIgnoreCase(LazyStringValue other)
        {
            return CompareToOrdinalIgnoreCase(this.Buffer, Size, other.Buffer, other.Size) == 0;
        }


        public static int CompareToOrdinalIgnoreCase(byte* strA, int strALen, byte* strB, int strBLen)
        {
            int length = Math.Min(strALen, strBLen);
            {
                byte* a = strA;
                byte* b = strB;

                while (length != 0 && (*a <= 0x7F) && (*b <= 0x7F))
                {
                    int charA = *a;
                    int charB = *b;

                    if (charA == charB)
                    {
                        a++;
                        b++;
                        length--;
                        continue;
                    }

                    // uppercase both chars - notice that we need just one compare per char
                    if ((uint)(charA - 'a') <= 'z' - 'a')
                        charA -= 0x20;
                    if ((uint)(charB - 'a') <= 'z' - 'a')
                        charB -= 0x20;

                    // Return the (case-insensitive) difference between them.
                    if (charA != charB)
                        return charA - charB;

                    // Next char
                    a++;
                    b++;
                    length--;
                }

                if (length == 0)
                    return strALen - strBLen;

                return string.Compare(
                    Encoding.UTF8.GetString(strA, strALen),
                    Encoding.UTF8.GetString(strB, strBLen),
                    StringComparison.OrdinalIgnoreCase);
            }
        }

        public Span<byte> AsSpan()
        {
            return new Span<byte>(_buffer, _size);
        }

        public ReadOnlySpan<byte> AsReadOnlySpan()
        {
            return new ReadOnlySpan<byte>(_buffer, _size);
        }


        public void CopyTo(byte* dest)
        {
            Memory.Copy(dest, _buffer, _size);
        }

        public LazyStringValue Clone(JsonOperationContext context)
        {
            if (_size == 0)
                return context.Empty;

            return context.GetLazyString(_buffer, _size, longLived: false);
        }

        public LazyStringValue CloneOnSameContext()
        {
            if (_size == 0)
                return _context.Empty;

            return _context.GetLazyString(_buffer, _size, longLived: false);
        }

        public bool HasStringValue => _string != null;

        [ThreadStatic]
        private static char[] _lazyStringTempBuffer;

        [ThreadStatic]
        private static byte[] _lazyStringTempComparisonBuffer;

        public int[] EscapePositions;
        public AllocatedMemoryData AllocatedMemoryData;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LazyStringValue(string str, byte* buffer, int size, JsonOperationContext context)
        {
            Debug.Assert(context != null);
            _context = context;
            _size = size;
            _buffer = buffer;
            _string = str;
            _length = -1;
        }

        static LazyStringValue()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += CleanBuffers;
        }

        public static void CleanBuffers()
        {
            _lazyStringTempBuffer = null;
            _lazyStringTempComparisonBuffer = null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(string other)
        {
            DisposableExceptions.ThrowIfDisposedOnDebug(this);

            if (_string != null)
                return string.Equals(_string, other, StringComparison.Ordinal);


            if (_lazyStringTempComparisonBuffer == null || _lazyStringTempComparisonBuffer.Length < other.Length * 5)
            {
                var sizeInBytes = Encodings.Utf8.GetMaxByteCount(other.Length);

                if (_lazyStringTempComparisonBuffer == null || _lazyStringTempComparisonBuffer.Length < other.Length)
                    _lazyStringTempComparisonBuffer = new byte[Bits.PowerOf2(sizeInBytes)];
            }

            var tmpSize = Encodings.Utf8.GetBytes(other.AsSpan(), _lazyStringTempComparisonBuffer);
            if (Size != tmpSize)
                return false;

            return Memory.CompareInline(new ReadOnlySpan<byte>(Buffer, tmpSize), _lazyStringTempComparisonBuffer.AsSpan(), tmpSize) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(LazyStringValue other)
        {
            DisposableExceptions.ThrowIfDisposedOnDebug(this);

            int size = Size;
            if (other.Size != size)
                return false;

            return Memory.CompareInline(Buffer, other.Buffer, size) == 0;
        }

        public int CompareTo(string other)
        {
            if (_string != null)
                return string.Compare(_string, other, StringComparison.Ordinal);

            var sizeInBytes = Encodings.Utf8.GetMaxByteCount(other.Length);

            if (_lazyStringTempComparisonBuffer == null || _lazyStringTempComparisonBuffer.Length < other.Length)
                _lazyStringTempComparisonBuffer = new byte[Bits.PowerOf2(sizeInBytes)];

            var otherSize = Encodings.Utf8.GetBytes(other.AsSpan(), _lazyStringTempComparisonBuffer);
            var result = Memory.CompareInline(new ReadOnlySpan<byte>(Buffer, Length), _lazyStringTempComparisonBuffer, Math.Min(Size, otherSize));
            return result == 0 ? Size - otherSize : result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareTo(LazyStringValue other)
        {
            if (other.Buffer == Buffer && other.Size == Size)
                return 0;
            return Compare(other.Buffer, other.Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(byte* other, int otherSize)
        {
            DisposableExceptions.ThrowIfDisposedOnDebug(this);

            int size = Size;
            var result = Memory.CompareInline(Buffer, other, Math.Min(size, otherSize));
            return result == 0 ? size - otherSize : result;
        }

        public static bool operator !=(LazyStringValue self, LazyStringValue str) => !(self == str);

        public static bool operator !=(LazyStringValue self, string str) => !(self == str);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator ==(LazyStringValue self, string str)
        {
            if (ReferenceEquals(self, null) && str == null)
                return true;
            if (ReferenceEquals(self, null) || str == null)
                return false;
            return self.Equals(str);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator ==(LazyStringValue self, LazyStringValue str)
        {
            if (ReferenceEquals(self, str))
                return true;

            if (ReferenceEquals(self, null))
                return false;
            return !ReferenceEquals(str, null) && self.Equals(str);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator string(LazyStringValue self)
        {
            if (self == null)
                return null;

            DisposableExceptions.ThrowIfDisposedOnDebug(self);

            return self._string ??
                   (self._string = Encodings.Utf8.GetString(self._buffer, self._size));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator byte[](LazyStringValue self)
        {
            var valueAsString = (string)self;
            return Convert.FromBase64String(valueAsString);
        }

        public static explicit operator short(LazyStringValue self)
        {
            var valueAsString = (string)self;
            if (short.TryParse(valueAsString, NumberStyles.Any, CultureInfo.InvariantCulture, out short result))
                return result;

            throw new InvalidCastException($"Couldn't convert {valueAsString} (LazyStringValue) to int16");
        }

        public static explicit operator int(LazyStringValue self)
        {
            var valueAsString = (string)self;
            if (int.TryParse(valueAsString, NumberStyles.Any, CultureInfo.InvariantCulture, out int result))
                return result;

            throw new InvalidCastException($"Couldn't convert {valueAsString} (LazyStringValue) to int32");
        }

        public static explicit operator long(LazyStringValue self)
        {
            var valueAsString = (string)self;
            if (long.TryParse(valueAsString, NumberStyles.Any, CultureInfo.InvariantCulture, out long result))
                return result;

            throw new InvalidCastException($"Couldn't convert {valueAsString} (LazyStringValue) to int64");
        }

        public static explicit operator decimal(LazyStringValue self)
        {
            var valueAsString = (string)self;
            if (decimal.TryParse(valueAsString, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal result))
                return result;

            throw new InvalidCastException($"Couldn't convert {valueAsString} (LazyStringValue) to decimal");
        }

        public static explicit operator double(LazyStringValue self)
        {
            var valueAsString = (string)self;
            if (double.TryParse(valueAsString, NumberStyles.Any, CultureInfo.InvariantCulture, out double result))
                return result;

            throw new InvalidCastException($"Couldn't convert {valueAsString} (LazyStringValue) to double");
        }

        public static explicit operator float(LazyStringValue self)
        {
            var valueAsString = (string)self;
            if (float.TryParse(valueAsString, NumberStyles.Any, CultureInfo.InvariantCulture, out float result))
                return result;

            throw new InvalidCastException($"Couldn't convert {valueAsString} (LazyStringValue) to float");
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IEnumerator<char> GetEnumerator()
        {
            return ToString().GetEnumerator();
        }

        public override bool Equals(object other)
        {
            DisposableExceptions.ThrowIfDisposedOnDebug(this);

            if (ReferenceEquals(other, this))
                return true;

            switch (other)
            {
                case null:
                    return false;

                case string str:
                    return Equals(str);

                case LazyStringValue lsv:
                    return lsv.Equals(this);

                case LazyCompressedStringValue lcsv:
                    return lcsv.ToLazyStringValue().Equals(this);
            }

            return false;
        }

        private int? _hashCode;

        public override int GetHashCode()
        {
            DisposableExceptions.ThrowIfDisposedOnDebug(this);

            if (_hashCode.HasValue)
                return _hashCode.Value;

            _hashCode = IntPtr.Size == 4
                ? (int)Hashing.XXHash32.CalculateInline(Buffer, Size)
                : (int)Hashing.XXHash64.CalculateInline(Buffer, (ulong)Size);

            return _hashCode.Value;
        }

        public override string ToString()
        {
            return (string)this; // invoke the implicit string conversion
        }

        public int CompareTo(object obj)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (obj == null)
                return 1;

            var lsv = obj as LazyStringValue;

            if (lsv != null)
                return CompareTo(lsv);

            if (obj is string s)
                return CompareTo(s);

            throw new NotSupportedException($"Cannot compare LazyStringValue to object of type {obj.GetType().Name}");
        }

        public bool IsDisposed { get; internal set; }

        public void Dispose()
        {
            DisposableExceptions.ThrowIfDisposed(this);

            ReturnAllocatedMemory();

            IsDisposed = true;
        }

        private void ReturnAllocatedMemory()
        {
            if (AllocatedMemoryData == null)
                return;

            if (_context.Generation == AllocatedMemoryData.ContextGeneration)
            {
                _context.ReturnMemory(AllocatedMemoryData);
            }

            AllocatedMemoryData = null;
        }

        public bool Contains(char value)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (_string != null)
                return _string.Contains(value);

            return ToString().Contains(value);
        }

#if NETCOREAPP3_1_OR_GREATER

        public bool Contains(char value, StringComparison comparisonType)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (_string != null)
                return _string.Contains(value, comparisonType);

            return ToString().Contains(value, comparisonType);
        }

#endif

        public bool Contains(string value)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (_string != null)
                return _string.Contains(value);

            return ToString().Contains(value);
        }

#if NETCOREAPP3_1_OR_GREATER

        public bool Contains(string value, StringComparison comparisonType)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (_string != null)
                return _string.Contains(value, comparisonType);

            return ToString().Contains(value, comparisonType);
        }

#endif

        public bool EndsWith(string value)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (_string != null)
                return _string.EndsWith(value);

            PortableExceptions.ThrowIfNull(value);

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
            DisposableExceptions.ThrowIfDisposed(this);

            if (value.Size > Size)
                return false;

            return Memory.Compare(Buffer + (Size - value.Size), value.Buffer, value.Size) == 0;
        }

        public bool EndsWith(string value, StringComparison comparisonType)
        {
            return ToString().EndsWith(value, comparisonType);
        }

#if !NETSTANDARD1_3

        public bool EndsWith(string value, bool ignoreCase, CultureInfo culture)
        {
            return ToString().EndsWith(value, ignoreCase, culture);
        }

#endif

        public bool EndsWith(char value)
        {
            return EndsWith(value.ToString());
        }

        public int IndexOf(char value)
        {
            return IndexOf(value, 0, Length);
        }

#if NETCOREAPP3_1_OR_GREATER

        public int IndexOf(char value, StringComparison comparisonType)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (_string != null)
                return _string.IndexOf(value, comparisonType);

            return ToString().IndexOf(value, comparisonType);
        }

#endif

        public int IndexOf(char value, int startIndex)
        {
            return IndexOf(value, startIndex, Length - startIndex);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ValidateIndexes(int startIndex, int count)
        {
            if (startIndex < 0 || count < 0)
                throw new ArgumentOutOfRangeException($"count ({count}) or startIndex ({startIndex}) is negative.");

            if (startIndex > Length)
                throw new ArgumentOutOfRangeException(nameof(startIndex), startIndex, $"startIndex ({startIndex}) is greater than the length of this string.");

            if (count > Length - startIndex)
                throw new ArgumentOutOfRangeException(nameof(count), count, $"count ({count}) is greater than the length of this string minus startIndex.");
        }

        public int IndexOf(char value, int startIndex, int count)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (_string != null)
                return _string.IndexOf(value, startIndex, count);

            ValidateIndexes(startIndex, count);

            if (_lazyStringTempBuffer == null || _lazyStringTempBuffer.Length < Length)
                _lazyStringTempBuffer = new char[Bits.PowerOf2(Length)];

            fixed (char* pChars = _lazyStringTempBuffer)
                Encodings.Utf8.GetChars(Buffer, Size, pChars, Length);

            for (int i = startIndex; i < startIndex + count; i++)
            {
                if (_lazyStringTempBuffer[i] == value)
                    return i;
            }

            return -1;
        }

        public int IndexOf(string value)
        {
            return ToString().IndexOf(value, StringComparison.Ordinal);
        }

        public int IndexOf(string value, int startIndex)
        {
            return ToString().IndexOf(value, startIndex);
        }

        public int IndexOf(string value, int startIndex, int count)
        {
            return ToString().IndexOf(value, startIndex, count);
        }

        public int IndexOf(string value, int startIndex, StringComparison comparisonType)
        {
            return ToString().IndexOf(value, startIndex, comparisonType);
        }

        public int IndexOf(string value, int startIndex, int count, StringComparison comparisonType)
        {
            return ToString().IndexOf(value, startIndex, count, comparisonType);
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
            DisposableExceptions.ThrowIfDisposed(this);

            if (_string != null)
                return _string.IndexOfAny(anyOf, startIndex, count);

            ValidateIndexes(startIndex, count);

            if (_lazyStringTempBuffer == null || _lazyStringTempBuffer.Length < Length)
                _lazyStringTempBuffer = new char[Bits.PowerOf2(Length)];

            fixed (char* pChars = _lazyStringTempBuffer)
                Encodings.Utf8.GetChars(Buffer, Size, pChars, Length);

            for (int i = startIndex; i < startIndex + count; i++)
            {
                if (anyOf.Contains(_lazyStringTempBuffer[i]))
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
            return LastIndexOf(value, Length - 1, Length);
        }

        public int LastIndexOf(char value, int startIndex)
        {
            return LastIndexOf(value, startIndex, startIndex);
        }

        public int LastIndexOf(char value, int startIndex, int count)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (_string != null)
                return _string.LastIndexOf(value, startIndex, count);

            ValidateIndexes(Length - startIndex - 1, count);

            if (_lazyStringTempBuffer == null || _lazyStringTempBuffer.Length < Length)
                _lazyStringTempBuffer = new char[Bits.PowerOf2(Length)];

            fixed (char* pChars = _lazyStringTempBuffer)
                Encodings.Utf8.GetChars(Buffer, Size, pChars, Length);

            for (int i = startIndex; i > startIndex - count; i--)
            {
                if (_lazyStringTempBuffer[i] == value)
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

        public int LastIndexOf(string value, int startIndex)
        {
            return ToString().LastIndexOf(value, startIndex);
        }

        public int LastIndexOf(string value, int startIndex, int count)
        {
            return ToString().LastIndexOf(value, startIndex, count);
        }

        public int LastIndexOf(string value, int startIndex, StringComparison comparisonType)
        {
            return ToString().LastIndexOf(value, startIndex, comparisonType);
        }

        public int LastIndexOf(string value, int startIndex, int count, StringComparison comparisonType)
        {
            return ToString().LastIndexOf(value, startIndex, count, comparisonType);
        }

        public int LastIndexOfAny(char[] anyOf)
        {
            return LastIndexOfAny(anyOf, Length - 1, Length);
        }

        public int LastIndexOfAny(char[] anyOf, int startIndex)
        {
            return LastIndexOfAny(anyOf, startIndex, startIndex);
        }

        public int LastIndexOfAny(char[] anyOf, int startIndex, int count)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (_string != null)
                return _string.LastIndexOfAny(anyOf, startIndex, count);

            ValidateIndexes(Length - startIndex - 1, count);

            if (_lazyStringTempBuffer == null || _lazyStringTempBuffer.Length < Length)
                _lazyStringTempBuffer = new char[Bits.PowerOf2(Length)];

            fixed (char* pChars = _lazyStringTempBuffer)
                Encodings.Utf8.GetChars(Buffer, Size, pChars, Length);

            for (int i = startIndex; i > startIndex - count; i--)
            {
                if (anyOf.Contains(_lazyStringTempBuffer[i]))
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

        public string Replace(string oldValue, string newValue, bool ignoreCase, CultureInfo culture)
        {
            throw new NotSupportedException();
        }

        public string Replace(string oldValue, string newValue, StringComparison comparisonType)
        {
            throw new NotSupportedException();
        }

        public string Substring(int startIndex)
        {
            return ToString().Substring(startIndex);
        }

        public string Substring(int startIndex, int length)
        {
            return ToString().Substring(startIndex, length);
        }

        public string Normalize()
        {
            return ToString().Normalize();
        }

        public string Normalize(NormalizationForm normalizationForm)
        {
            return ToString().Normalize(normalizationForm);
        }

        public bool IsNormalized()
        {
            return ToString().IsNormalized();
        }

        public bool IsNormalized(NormalizationForm normalizationForm)
        {
            return ToString().IsNormalized(normalizationForm);
        }

        public string[] Split(char separator, StringSplitOptions options = StringSplitOptions.None)
        {
            return Split(new[] { separator }, options);
        }

        public string[] Split(char separator, int count, StringSplitOptions options = StringSplitOptions.None)
        {
            return ToString().Split(new[] { separator }, count, options);
        }

        public string[] Split(string separator, StringSplitOptions options = StringSplitOptions.None)
        {
            return Split(new[] { separator }, options);
        }

        public string[] Split(string separator, int count, StringSplitOptions options = StringSplitOptions.None)
        {
            return ToString().Split(new[] { separator }, count, options);
        }

        public string[] Split(char[] separator)
        {
            return ToString().Split(separator, StringSplitOptions.None);
        }

        public string[] Split(char[] separator, int count)
        {
            return ToString().Split(separator, count, StringSplitOptions.None);
        }

        public string[] Split(char[] separator, StringSplitOptions options)
        {
            return ToString().Split(separator, options);
        }

        public string[] Split(char[] separator, int count, StringSplitOptions options)
        {
            return ToString().Split(separator, count, options);
        }

        public string[] Split(string[] separator, StringSplitOptions options = StringSplitOptions.None)
        {
            return ToString().Split(separator, options);
        }

        public string[] Split(string[] separator, int count, StringSplitOptions options = StringSplitOptions.None)
        {
            return ToString().Split(separator, count, options);
        }

#if NET9_0_OR_GREATER
        public string[] Split(scoped ReadOnlySpan<char> separator)
        {
            return ToString().Split(separator);
        }
#endif

        public bool StartsWith(string value)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (_string != null)
                return _string.StartsWith(value);

            PortableExceptions.ThrowIfNull(value);

            // Every UTF8 character uses at least 1 byte
            if (value.Length == 0)
                return true;
            if (value.Length > Size)
                return false;

            // We are assuming these values are going to be relatively constant throughout the object lifespan
            LazyStringValue converted = _context.GetLazyStringForFieldWithCaching(value);
            return StartsWith(converted);
        }

        public bool StartsWith(LazyStringValue value)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (value.Size > Size)
                return false;

            return Memory.Compare(Buffer, value.Buffer, value.Size) == 0;
        }

        public bool StartsWith(ReadOnlySpan<byte> value)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (value.Length > Size)
                return false;

            return AsSpan().StartsWith(value);
        }


        public bool StartsWith(string value, StringComparison comparisionType)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (value.Length > Size)
                return false;

            return ToString().StartsWith(value, comparisionType);
        }

#if !NETSTANDARD1_3

        public bool StartsWith(string value, bool ignoreCase, CultureInfo culture)
        {
            DisposableExceptions.ThrowIfDisposed(this);

            if (value.Length > Size)
                return false;

            return ToString().StartsWith(value, ignoreCase, culture);
        }

#endif

        public bool StartsWith(char value)
        {
            return StartsWith(value.ToString());
        }

        public char[] ToArray()
        {
            return ToString().ToArray();
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

#if !NETSTANDARD1_3

        public string ToLower(CultureInfo culture)
        {
            return ToString().ToLower(culture);
        }

#endif

        public string ToLowerInvariant()
        {
            return ToString().ToLowerInvariant();
        }

        public string ToUpper()
        {
            return ToString().ToUpper();
        }

#if !NETSTANDARD1_3

        public string ToUpper(CultureInfo culture)
        {
            return ToString().ToUpper(culture);
        }

#endif

        public string ToUpperInvariant()
        {
            return ToString().ToUpperInvariant();
        }

        public string Trim()
        {
            return ToString().Trim();
        }

        public string Trim(char trimChar)
        {
            return ToString().Trim(trimChar);
        }

        public string Trim(params char[] trimChars)
        {
            return ToString().Trim(trimChars);
        }

#if NET9_0_OR_GREATER
        public string Trim(scoped ReadOnlySpan<char> trimChars)
        {
            return ToString().Trim(trimChars);
        }
#endif

        public string TrimEnd()
        {
            return ToString().TrimEnd();
        }

        public string TrimEnd(char trimChar)
        {
            return ToString().TrimEnd(trimChar);
        }

        public string TrimEnd(params char[] trimChars)
        {
            return ToString().TrimEnd(trimChars);
        }

#if NET9_0_OR_GREATER
        public string TrimEnd(scoped ReadOnlySpan<char> trimChars)
        {
            return ToString().TrimEnd(trimChars);
        }
#endif

        public string TrimStart()
        {
            return ToString().TrimStart();
        }

        public string TrimStart(char trimChar)
        {
            return ToString().TrimStart(trimChar);
        }

        public string TrimStart(params char[] trimChars)
        {
            return ToString().TrimStart(trimChars);
        }

#if NET9_0_OR_GREATER
        public string TrimStart(scoped ReadOnlySpan<char> trimChars)
        {
            return ToString().TrimStart(trimChars);
        }
#endif

        public string Reverse()
        {
            DisposableExceptions.ThrowIfDisposed(this);

            var maxCharCount = Encodings.Utf8.GetMaxCharCount(Length);
            if (_lazyStringTempBuffer == null || _lazyStringTempBuffer.Length < maxCharCount)
                _lazyStringTempBuffer = new char[Bits.PowerOf2(maxCharCount)];

            var buffer = _buffer;

            // in case we received a string, with no _buffer
            if (buffer == null)
            {
                fixed (char* stringBuffer = _string)
                {
                    buffer = (byte*)stringBuffer;
                    return GetReversedStringFromBuffer(buffer);
                }
            }

            return GetReversedStringFromBuffer(buffer);
        }

        private string GetReversedStringFromBuffer(byte* buffer)
        {
            fixed (char* pChars = _lazyStringTempBuffer)
            {
                var chars = Encodings.Utf8.GetChars(buffer, Length, pChars, _lazyStringTempBuffer.Length);
                Array.Reverse(_lazyStringTempBuffer, 0, chars);
                return new string(_lazyStringTempBuffer, 0, chars);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Renew(string str, byte* buffer, int size, JsonOperationContext context)
        {
            Debug.Assert(size >= 0);

            // PERF: Renewing a LazyStringValue that has been used by the current context, and it is still valid
            // makes no sense, let's just use it and be done with it.
            if (AllocatedMemoryData != null)
            {
                if (_context != context || _context.Generation != AllocatedMemoryData.ContextGeneration)
                {
                    // However it can happen that it is invalid. Either by being allocated by some other context OR
                    // by the context have been renewed.
                    AllocatedMemoryData = null;
                }
            }

            _size = size;
            _buffer = buffer;
            _string = str;
            _length = -1;
            EscapePositions = null;
            IsDisposed = false;

            _hashCode = default;
            _context = context;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsControlCodeCharacter(out byte b)
        {
            Debug.Assert(Size == 1);

            b = Buffer[0];
            // control code characters
            return b < 32 || (b >= 127 && b <= 159);
        }

        public TypeCode GetTypeCode()
        {
            return MaterializeStringValue.GetTypeCode();
        }

        public bool ToBoolean(IFormatProvider provider)
        {
            return ((IConvertible)MaterializeStringValue).ToBoolean(provider);
        }

        public byte ToByte(IFormatProvider provider)
        {
            return ((IConvertible)MaterializeStringValue).ToByte(provider);
        }

        public char ToChar(IFormatProvider provider)
        {
            return ((IConvertible)MaterializeStringValue).ToChar(provider);
        }

        public DateTime ToDateTime(IFormatProvider provider)
        {
            return ((IConvertible)MaterializeStringValue).ToDateTime(provider);
        }

        public decimal ToDecimal(IFormatProvider provider)
        {
            return ((IConvertible)MaterializeStringValue).ToDecimal(provider);
        }

        public double ToDouble(IFormatProvider provider)
        {
            return ((IConvertible)MaterializeStringValue).ToDouble(provider);
        }

        public short ToInt16(IFormatProvider provider)
        {
            return ((IConvertible)MaterializeStringValue).ToInt16(provider);
        }

        public int ToInt32(IFormatProvider provider)
        {
            return ((IConvertible)MaterializeStringValue).ToInt32(provider);
        }

        public long ToInt64(IFormatProvider provider)
        {
            return ((IConvertible)MaterializeStringValue).ToInt64(provider);
        }

        public sbyte ToSByte(IFormatProvider provider)
        {
            return ((IConvertible)MaterializeStringValue).ToSByte(provider);
        }

        public float ToSingle(IFormatProvider provider)
        {
            return ((IConvertible)MaterializeStringValue).ToSingle(provider);
        }

        public string ToString(IFormatProvider provider)
        {
            return MaterializeStringValue.ToString(provider);
        }

        public object ToType(Type conversionType, IFormatProvider provider)
        {
            if (conversionType.IsEnum)
                return Enum.Parse(conversionType, MaterializeStringValue);

            return ((IConvertible)MaterializeStringValue).ToType(conversionType, provider);
        }

        public ushort ToUInt16(IFormatProvider provider)
        {
            return ((IConvertible)MaterializeStringValue).ToUInt16(provider);
        }

        public uint ToUInt32(IFormatProvider provider)
        {
            return ((IConvertible)MaterializeStringValue).ToUInt32(provider);
        }

        public ulong ToUInt64(IFormatProvider provider)
        {
            return ((IConvertible)MaterializeStringValue).ToUInt64(provider);
        }

        public bool StartsWithOrdinalIgnoreCase(LazyStringValue prefix)
        {
            if (prefix.Size > Size)
                return false;

            return CompareToOrdinalIgnoreCase(this.Buffer, prefix.Size,
                       prefix.Buffer, prefix.Size) == 0;
        }

        public void Truncate(int size)
        {
            _size = size;
            _length = -1;
            _string = null;
        }
    }
}
