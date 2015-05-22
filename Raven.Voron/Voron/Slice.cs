using System;
using System.Diagnostics;
using System.Text;
using Voron.Impl;
using Voron.Trees;
using Voron.Util.Conversion;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Voron.Util;

namespace Voron
{
	public sealed unsafe class Slice : MemorySlice
	{        
		public static Slice AfterAllKeys = new Slice(SliceOptions.AfterAllKeys);
		public static Slice BeforeAllKeys = new Slice(SliceOptions.BeforeAllKeys);
		public static Slice Empty = new Slice(new byte[0]);

        internal byte[] Array;
		internal byte* Pointer;

		public Slice(SliceOptions options) : base( options )
		{}

		public Slice(byte* key, ushort size) 
            : base( SliceOptions.Key, size, size )
		{
			this.Pointer = key;
		}

        public Slice(byte[] key)
            : base(SliceOptions.Key, (ushort)key.Length)
		{
            this.Array = key;
		}

		public Slice(Slice other, ushort size) 
            : base ( other.Options, size, size )
		{
            Array = other.Array;
            Pointer = other.Pointer;
		}

		public Slice(byte[] key, ushort size) 
            : base ( SliceOptions.Key, size, size )
		{
            Debug.Assert(key != null);
			Array = key;
		}

		public Slice(NodeHeader* node)
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


		public override string ToString()
		{
			// this is used for debug purposes only
			if (Options != SliceOptions.Key)
				return Options.ToString();

			if (Size == sizeof(long) && Debugger.IsAttached)
			{
				if (Array != null)
				{
					if(Array[0] == 0)
						return "I64 = " +  EndianBitConverter.Big.ToInt64(Array,0);
				}
				else if (*Pointer == 0)
				{
					var bytes = new byte[sizeof(long)];
					CopyTo(bytes);
					return "I64 = " + EndianBitConverter.Big.ToInt64(bytes, 0);
				}
			}

			if (Array != null)
				return Encoding.UTF8.GetString(Array,0, Size);

			return new string((sbyte*)Pointer, 0, Size, Encoding.UTF8);
		}


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int CompareDataInline(Slice other, ushort size)
        {
            if (Array != null)
            {
                fixed (byte* a = Array)
                {
                    if (other.Array != null)
                    {
                        fixed (byte* b = other.Array)
                        {
                            return MemoryUtils.CompareInline(a, b, size);
                        }
                    }
                    else return MemoryUtils.CompareInline(a, other.Pointer, size);
                }
            }

            if (other.Array != null)
            {
                fixed (byte* b = other.Array)
                {
                    return MemoryUtils.CompareInline(Pointer, b, size);
                }
            }
            else return MemoryUtils.CompareInline(Pointer, other.Pointer, size);
        }

        protected override int CompareData(MemorySlice other, ushort size)
		{
            var otherSlice = other as Slice;
			if (otherSlice != null)
                return CompareDataInline(otherSlice, size);

			var prefixedSlice = other as PrefixedSlice;
			if (prefixedSlice != null)
				return PrefixedSliceComparisonMethods.Compare(this, prefixedSlice, MemoryUtils.MemoryComparerInstance, size);

			throw new NotSupportedException("Cannot compare because of unknown slice type: " + other.GetType());
		}      

		protected override int CompareData(MemorySlice other, PrefixedSliceComparer cmp, ushort size)
		{
			var otherSlice = other as Slice;

			if (otherSlice != null)
			{
				if (Array != null)
				{
					fixed (byte* a = Array)
					{
						if (otherSlice.Array != null)
						{
							fixed (byte* b = otherSlice.Array)
							{
								return cmp(a, b, size);
							}
						}
						return cmp(a, otherSlice.Pointer, size);
					}
				}

				if (otherSlice.Array != null)
				{
					fixed (byte* b = otherSlice.Array)
					{
						return cmp(Pointer, b, size);
					}
				}

				return cmp(Pointer, otherSlice.Pointer, size);
			}

			var prefixedSlice = other as PrefixedSlice;

			if (prefixedSlice != null)
				return PrefixedSliceComparisonMethods.Compare(this, prefixedSlice, cmp, size);

			throw new NotSupportedException("Cannot compare because of unknown slice type: " + other.GetType());
		}

		public static implicit operator Slice(string s)
		{
			return new Slice(Encoding.UTF8.GetBytes(s));
		}

		public override void CopyTo(byte* dest)
		{
			if (Array == null)
			{
                MemoryUtils.Copy(dest, Pointer, Size);
				return;
			}
			fixed (byte* a = Array)
			{
                MemoryUtils.Copy(dest, a, Size);
			}
		}

		public override Slice ToSlice()
		{
			return new Slice(this, Size);
		}

		public void CopyTo(byte[] dest)
		{
			if (Array == null)
			{
				fixed (byte* p = dest)
                    MemoryUtils.Copy(p, Pointer, Size);
				return;
			}
			Buffer.BlockCopy(Array, 0, dest, 0, Size);
		}

		public void CopyTo(int from, byte[] dest, int offset, int count)
		{
			if (from + count > Size)
				throw new ArgumentOutOfRangeException("from", "Cannot copy data after the end of the slice");
			if(offset + count > dest.Length)
				throw new ArgumentOutOfRangeException("from", "Cannot copy data after the end of the buffer" +
				                                              "");
			if (Array == null)
			{
				fixed (byte* p = dest)
                    MemoryUtils.Copy(p + offset, Pointer + from, count);
				return;
			}
			Buffer.BlockCopy(Array, from, dest, offset, count);
		}

		public void CopyTo(int from, byte* dest, int offset, int count)
		{
			if (from + count > Size)
				throw new ArgumentOutOfRangeException("from", "Cannot copy data after the end of the slice");

			if (Array == null)
			{
                MemoryUtils.Copy(dest + offset, Pointer + from, count);
				return;
			}

			fixed (byte* p = Array)
                MemoryUtils.Copy(dest + offset, p + from, count);
		}

		public Slice Clone()
		{
			var buffer = new byte[Size];
			if (Array == null)
			{
				fixed (byte* dest = buffer)
				{
                    MemoryUtils.Copy(dest, Pointer, Size);
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
                return new ValueReader(Array, Size);

	        return new ValueReader(Pointer, Size);
	    }

		public override Slice Skip(ushort bytesToSkip)
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

		public override void PrepareForSearching()
		{
			PrefixComparisonCache = new PrefixComparisonCache();
		}

		public PrefixComparisonCache PrefixComparisonCache;

		public new ushort FindPrefixSize(MemorySlice other)
		{
			if (PrefixComparisonCache == null)
				return base.FindPrefixSize(other);

			PrefixComparisonCache.Disabled = true;
			try
			{
				return base.FindPrefixSize(other);
			}
			finally
			{
				PrefixComparisonCache.Disabled = false;
			}
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
        public static void SetInline(Slice slice, NodeHeader* node)
        {
            slice.Pointer = (byte*)node + Constants.NodeHeaderSize;
            slice.Size = node->KeySize;
            slice.KeyLength = node->KeySize;
            slice.Array = null;
        }
		
		public override void Set(NodeHeader* node)
		{
            SetInline(this, node);
		}
	}
}