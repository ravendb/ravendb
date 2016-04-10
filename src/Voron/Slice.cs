using System;
using System.Diagnostics;
using System.Text;
using Voron.Impl;
using Voron.Util.Conversion;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Voron.Util;
using Sparrow;
using Voron.Data.BTrees;
using Sparrow.Binary;

namespace Voron
{
    public sealed unsafe class Slice 
    {        
        public static Slice AfterAllKeys = new Slice(SliceOptions.AfterAllKeys);
        public static Slice BeforeAllKeys = new Slice(SliceOptions.BeforeAllKeys);
        public static Slice Empty = new Slice(new byte[0]);

        internal byte[] Array;
        internal byte* Pointer;

        public ushort Size;
        public ushort KeyLength;
        public SliceOptions Options;


        private Slice()
        { }

        public Slice(SliceOptions options)
        {
            this.Options = options;
        }

        private Slice(SliceOptions options, ushort size)
        {
            this.Options = options;
            this.Size = size;
            this.KeyLength = size;
        }

        private Slice(SliceOptions options, ushort size, ushort keyLength)
        {
            this.Options = options;
            this.Size = size;
            this.KeyLength = keyLength;
        }

        internal BitVector ToBitVector()
        {
            BitVector bitVector;
            if (Array != null)
            {
                bitVector = BitVector.Of(true, Array);
            }
            else
            {
                bitVector = BitVector.Of(true, this.Pointer, this.KeyLength);
            }

            ValidateBitVectorIsPrefixFree(bitVector);

            return bitVector;
        }

        [Conditional("DEBUG")]
        private void ValidateBitVectorIsPrefixFree(BitVector vector)
        {
            int start = vector.Count - 2 * BitVector.BitsPerByte;
            for (int i = 0; i < 2 * BitVector.BitsPerByte; i++)
                Debug.Assert(vector.Get(start + i) == false);
        }

        public bool Equals(Slice other)
        {
            return Compare(other) == 0;
        }

        public Slice(byte* key, ushort size) 
            : this( SliceOptions.Key, size, size )
        {
            this.Pointer = key;
        }

        public Slice(byte[] key)
            : this(SliceOptions.Key, (ushort)key.Length)
        {
            this.Array = key;
        }

        public Slice(Slice other, ushort size) 
            : this( other.Options, size, size )
        {
            Array = other.Array;
            Pointer = other.Pointer;
        }

        public Slice(byte[] key, ushort size) 
            : this( SliceOptions.Key, size, size )
        {
            Debug.Assert(key != null);
            Array = key;
        }

        public Slice(TreeNodeHeader* node)
        {
            Options = SliceOptions.Key;
            SetInline(this, node);
        }

        public Slice(string key)
            : this(Encoding.UTF8.GetBytes(key))
        { }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Slice)obj);
        }

        public override int GetHashCode()
        {
            // Given how the size of slices can vary it is better to lose a bit (10%) on smaller slices 
            // (less than 20 bytes) and to win big on the bigger ones. 
            //
            // After 24 bytes the gain is 10%
            // After 64 bytes the gain is 2x
            // After 128 bytes the gain is 4x.
            //
            // We should control the distribution of this over time. 
            unsafe
            {
                if (Array != null)
                {
                    fixed (byte* arrayPtr = Array)
                    {
                        return (int)Hashing.XXHash32.CalculateInline(arrayPtr, Size);
                    }
                }
                else
                {
                    return (int)Hashing.XXHash32.CalculateInline(Pointer, Size);
                }
            }
        }

        public byte this[int index]
        {
            get
            {
                if (Array != null)
                    return Array[index];

                if(Pointer == null) //precaution
                    throw new InvalidOperationException("Uninitialized slice!");

                if(index < 0 || index > Size)
                    throw new ArgumentOutOfRangeException(nameof(index));

                return *(Pointer + (sizeof (byte)*index));
            }			
        }

        public override string ToString()
        {
            // this is used for debug purposes only
            if (Options != SliceOptions.Key)
                return Options.ToString();

            if (Size == sizeof (long) && Debugger.IsAttached)
            {
                if (Array != null)
                {
                    if (Array[0] == 0)
                        return "I64 = " + EndianBitConverter.Big.ToInt64(Array, 0);
                }
                else if (*Pointer == 0)
                {
                    var bytes = new byte[sizeof (long)];
                    CopyTo(bytes);
                    return "I64 = " + EndianBitConverter.Big.ToInt64(bytes, 0);
                }
            }

            if (Array != null)
            {
                if (Array.Length > 0 && Array[0] == 0)
                {
                    return ByteArrayToHexViaLookup32(Array);
                }
                return Encoding.UTF8.GetString(Array, 0, Size);
            }
            if (Size > 0 && Pointer[0] == 0)
            {
                return BytePointerToHexViaLookup32(Pointer, Size);
            }
            var temp = new byte[Size];
            CopyTo(temp);
            return Encoding.UTF8.GetString(temp, 0, Size);

        }

        private static readonly uint[] _lookup32 = CreateLookup32();

        private static uint[] CreateLookup32()
        {
            var result = new uint[256];
            for (int i = 0; i < 256; i++)
            {
                string s = i.ToString("X2");
                result[i] = ((uint)s[0]) + ((uint)s[1] << 16);
            }
            return result;
        }

        private static string ByteArrayToHexViaLookup32(byte[] bytes)
        {
            var lookup32 = _lookup32;
            var result = new char[bytes.Length * 2];
            for (int i = 0; i < bytes.Length; i++)
            {
                var val = lookup32[bytes[i]];
                result[2 * i] = (char)val;
                result[2 * i + 1] = (char)(val >> 16);
            }
            return new string(result);
        }


        private static string BytePointerToHexViaLookup32(byte* bytes, int count)
        {
            var lookup32 = _lookup32;
            var result = new char[count * 2];
            for (int i = 0; i < count; i++)
            {
                var val = lookup32[bytes[i]];
                result[2 * i] = (char)val;
                result[2 * i + 1] = (char)(val >> 16);
            }
            return new string(result);
        }

        private int CompareData(Slice other, ushort size)
        {
            if (Array != null)
            {
                fixed (byte* a = Array)
                {
                    if (other.Array != null)
                    {
                        fixed (byte* b = other.Array)
                        {
                            return Memory.CompareInline(a, b, size);
                        }
                    }
                    else return Memory.CompareInline(a, other.Pointer, size);
                }
            }

            if (other.Array != null)
            {
                fixed (byte* b = other.Array)
                {
                    return Memory.CompareInline(Pointer, b, size);
                }
            }
            else return Memory.CompareInline(Pointer, other.Pointer, size);
        }      


        public static implicit operator Slice(string s)
        {
            return new Slice(Encoding.UTF8.GetBytes(s));
        }

        public void CopyTo(byte* dest)
        {
            if (Array == null)
            {
                Memory.Copy(dest, Pointer, Size);
                return;
            }
            fixed (byte* a = Array)
            {
                Memory.Copy(dest, a, Size);
            }
        }

        public Slice ToSlice()
        {
            return new Slice(this, Size);
        }

        public void CopyTo(byte[] dest)
        {
            if (Array == null)
            {
                fixed (byte* p = dest)
                    Memory.Copy(p, Pointer, Size);
                return;
            }
            Buffer.BlockCopy(Array, 0, dest, 0, Size);
        }

        public void CopyTo(int from, byte[] dest, int offset, int count)
        {
            if (from + count > Size)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the slice");
            if(offset + count > dest.Length)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the buffer" +
                                                              "");
            if (Array == null)
            {
                fixed (byte* p = dest)
                    Memory.Copy(p + offset, Pointer + from, count);
                return;
            }
            Buffer.BlockCopy(Array, from, dest, offset, count);
        }

        public void CopyTo(int from, byte* dest, int offset, int count)
        {
            if (from + count > Size)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the slice");

            if (Array == null)
            {
                Memory.Copy(dest + offset, Pointer + from, count);
                return;
            }

            fixed (byte* p = Array)
                Memory.Copy(dest + offset, p + from, count);
        }

        public Slice Clone()
        {
            var buffer = new byte[Size];
            if (Array == null)
            {
                fixed (byte* dest = buffer)
                {
                    Memory.Copy(dest, Pointer, Size);
                }
            }
            else
            {
                Buffer.BlockCopy(Array, 0, buffer, 0, Size);
            }

            return new Slice(buffer);
        }

        public ValueReader CreateReader()
        {
            if(Array != null)
                throw new InvalidOperationException("Cannot create value reader from byte[]");

            return new ValueReader(Pointer, Size);
        }

        public Slice Skip(ushort bytesToSkip)
        {
            if (bytesToSkip == 0)
                return new Slice(this, Size);

            if (Pointer != null)
                return new Slice(Pointer + bytesToSkip, (ushort)(Size - bytesToSkip));

            var toAllocate = Size - bytesToSkip;
            var array = new byte[toAllocate];

            Buffer.BlockCopy(Array, bytesToSkip, array, 0, toAllocate);

            return new Slice(array);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(byte* p, ushort size)
        {
            Pointer = p;
            Size = size;
            KeyLength = size;
            Array = null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SetInline(Slice slice, TreeNodeHeader* node)
        {
            slice.Pointer = (byte*)node + Constants.NodeHeaderSize;
            slice.Size = node->KeySize;
            slice.KeyLength = node->KeySize;
            slice.Array = null;
        }
        
        public void Set(TreeNodeHeader* node)
        {
            SetInline(this, node);
        }

        public int Compare(Slice other)
        {
            Debug.Assert(Options == SliceOptions.Key);
            Debug.Assert(other.Options == SliceOptions.Key);

            var srcKey = this.KeyLength;
            var otherKey = other.KeyLength;
            var length = srcKey <= otherKey ? srcKey : otherKey;

            var r = CompareData(other, length);
            if (r != 0)
                return r;

            return srcKey - otherKey;
        }

        public bool StartsWith(Slice other)
        {
            if (KeyLength < other.KeyLength)
                return false;

            return CompareData(other, other.KeyLength) == 0;
        }

    }
}