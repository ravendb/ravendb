using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sparrow.Json
{
    public unsafe class LazyStringValue : IComparable<string>, IEquatable<string>,
        IComparable<LazyStringValue>, IEquatable<LazyStringValue>, IDisposable, IComparable
    {
        public readonly JsonOperationContext Context;
        public readonly byte* Buffer;
        public readonly int Size;
        public string String;
        public int[] EscapePositions;
        public AllocatedMemoryData AllocatedMemoryData;
        public int? LastFoundAt;

        public byte this[int index] => Buffer[index];

        public int Length => Size;

        public LazyStringValue(string str, byte* buffer, int size, JsonOperationContext context)
        {
            String = str;
            Size = size;
            Context = context;
            Buffer = buffer;
        }


        public int CompareTo(string other)
        {
            var sizeInBytes = Encoding.UTF8.GetMaxByteCount(other.Length);
            var tmp = Context.GetNativeTempBuffer(sizeInBytes);
            fixed (char* pOther = other)
            {
                var tmpSize = Context.Encoding.GetBytes(pOther, other.Length, tmp, sizeInBytes);
                return Compare(tmp, tmpSize);
            }
        }

        public int CompareTo(LazyStringValue other)
        {
            if (other.Buffer == Buffer && other.Size == Size)
                return 0;
            return Compare(other.Buffer, other.Size);
        }

        public bool Equals(string other)
        {
            return CompareTo(other) == 0;
        }

        public bool Equals(LazyStringValue other)
        {
            return CompareTo(other) == 0;
        }

        public int Compare(byte* other, int otherSize)
        {
            var result = Memory.CompareInline(Buffer, other, Math.Min(Size, otherSize));
            return result == 0 ? Size - otherSize : result;
        }

        public static bool operator ==(LazyStringValue self, LazyStringValue str)
        {
            if (ReferenceEquals(self, null) && ReferenceEquals(str,null))
                return true;
            if (ReferenceEquals(self, null) || ReferenceEquals(str,null))
                return false;
            return self.Equals(str);
        }

        public static bool operator !=(LazyStringValue self, LazyStringValue str)
        {
            return !(self == str);
        }

        public static bool operator ==(LazyStringValue self, string str)
        {
            if (ReferenceEquals(self, null) && str == null)
                return true;
            if (ReferenceEquals(self, null) || str == null)
                return false;
            return self.Equals(str);
        }

        public static bool operator !=(LazyStringValue self, string str)
        {
            return !(self == str);
        }

        public static implicit operator string(LazyStringValue self)
        {
            if (self == null)
                return null;

            self.Materialize();
            return self.String;
        }

        //materialize the lazy string for cases when we need to use it out of context
        //(where the allocated buffers become invalid/not available)
        public void Materialize()
        {
            if (String != null)
                return;

            var charCount = Context.Encoding.GetCharCount(Buffer, Size);
            var str = new string(' ', charCount);
            fixed (char* pStr = str)
            {
                Context.Encoding.GetChars(Buffer, Size, pStr, charCount);
                String = str;
            }
        }

        public override bool Equals(object obj)
        {
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
            return (int)Hashing.XXHash32.CalculateInline(Buffer, Size);
        }

        public override string ToString()
        {
            return (string)this; // invoke the implicit string conversion
        }

        public int CompareTo(object obj)
        {
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

        public void Dispose()
        {
            if (AllocatedMemoryData == null)
                return;
            Context.ReturnMemory(AllocatedMemoryData);
            AllocatedMemoryData = null;
        }

        public bool Contains(string value)
        {
            return ToString().Contains(value);
        }

        public bool EndsWith(string value)
        {
            return ToString().EndsWith(value);
        }

        public bool EndsWith(string value, StringComparison comparisonType)
        {
            return ToString().EndsWith(value, comparisonType);
        }

        public int IndexOf(char value)
        {
            return ToString().IndexOf(value);
        }

        public int IndexOf(char value, int startIndex)
        {
            return ToString().IndexOf(value, startIndex);
        }

        public int IndexOf(char value, int startIndex, int count)
        {
            return ToString().IndexOf(value, startIndex, count);
        }

        public int IndexOf(string value)
        {
            return ToString().IndexOf(value);
        }

        public int IndexOf(string value, StringComparison comparisonType)
        {
            return ToString().IndexOf(value, comparisonType);
        }

        public int IndexOfAny(char[] anyOf)
        {
            return ToString().IndexOfAny(anyOf);
        }

        public int IndexOfAny(char[] anyOf, int startIndex)
        {
            return ToString().IndexOfAny(anyOf, startIndex);
        }

        public int IndexOfAny(char[] anyOf, int startIndex, int count)
        {
            return ToString().IndexOfAny(anyOf, startIndex, count);
        }

        public string Insert(int startIndex, string value)
        {
            return ToString().Insert(startIndex, value);
        }

        public int LastIndexOf(char value)
        {
            return ToString().LastIndexOf(value);
        }

        public int LastIndexOf(char value, int startIndex)
        {
            return ToString().LastIndexOf(value, startIndex);
        }

        public int LastIndexOf(char value, int startIndex, int count)
        {
            return ToString().LastIndexOf(value, startIndex, count);
        }

        public int LastIndexOf(string value)
        {
            return ToString().LastIndexOf(value);
        }

        public int LastIndexOf(string value, StringComparison comparisonType)
        {
            return ToString().LastIndexOf(value, comparisonType);
        }

        public int LastIndexOfAny(char[] anyOf)
        {
            return ToString().LastIndexOfAny(anyOf);
        }

        public int LastIndexOfAny(char[] anyOf, int startIndex)
        {
            return ToString().LastIndexOfAny(anyOf, startIndex);
        }

        public int LastIndexOfAny(char[] anyOf, int startIndex, int count)
        {
            return ToString().LastIndexOfAny(anyOf, startIndex, count);
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
            return ToString().StartsWith(value);
        }

        public bool StartsWith(string value, StringComparison comparisonType)
        {
            return ToString().StartsWith(value, comparisonType);
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

        public IEnumerable<char> Reverse()
        {
            return ToString().Reverse();
        }
    }
}