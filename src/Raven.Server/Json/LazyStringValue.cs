using System;
using System.Text;

using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using Sparrow;

namespace Raven.Server.Json
{
    public unsafe class LazyStringValue : IComparable<string>, IEquatable<string>,
        IComparable<LazyStringValue>, IEquatable<LazyStringValue>
    {
        public readonly MemoryOperationContext Context;
        public readonly byte* Buffer;
        public int Size;
        public string String;
        public int[] EscapePositions;
        public UnmanagedBuffersPool.AllocatedMemoryData AllocatedMemoryData;
        public int? LastFoundAt;


        public byte this[int index] => Buffer[index];

        public LazyStringValue(string str, byte* buffer, int size, MemoryOperationContext context)
        {
            String = str;
            Size = size;
            Context = context;
            Buffer = buffer;
        }


        public int CompareTo(string other)
        {
            var sizeInBytes = Encoding.UTF8.GetMaxByteCount(other.Length);
            var tmp = Context.GetNativeTempBuffer(sizeInBytes, out sizeInBytes);
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
            var result = Memory.Compare(Buffer, other, Math.Min(Size, otherSize));

            return result == 0 ? Size - otherSize : result;
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
                throw new ArgumentNullException(nameof(self));

            if (self.String != null)
                return self.String;

            var charCount = self.Context.Encoding.GetCharCount(self.Buffer, self.Size);
            var str = new string(' ', charCount);
            fixed (char* pStr = str)
            {
                self.Context.Encoding.GetChars(self.Buffer, self.Size, pStr, charCount);
                self.String = str;
                return str;
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
            if (comparer != null)
                return Equals(comparer);

            return ReferenceEquals(obj, this);
        }

        public override int GetHashCode()
        {
            return (int)Hashing.XXHash64.CalculateInline(Buffer, Size);
        }

        public override string ToString()
        {
            return (string)this; // invoke the implicit string conversion
        }
    }
}