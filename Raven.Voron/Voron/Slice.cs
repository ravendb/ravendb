using System;
using System.Diagnostics;
using System.Text;
using Voron.Impl;
using Voron.Trees;
using Voron.Util.Conversion;

namespace Voron
{
	using System.Runtime.CompilerServices;
	using Util;

	public sealed unsafe class Slice : MemorySlice
	{
		public static Slice AfterAllKeys = new Slice(SliceOptions.AfterAllKeys);
		public static Slice BeforeAllKeys = new Slice(SliceOptions.BeforeAllKeys);
		public static Slice Empty = new Slice(new byte[0]);

		internal byte[] Array;
		internal byte* Pointer;

		public Slice(SliceOptions options)
		{
			Options = options;
			Pointer = null;
			Array = null;
			Size = 0;
			KeyLength = 0;
		}

		public Slice(byte* key, ushort size)
		{
			Size = size;
			KeyLength = size;
			Options = SliceOptions.Key;
			Array = null;
			Pointer = key;
		}

		public Slice(byte[] key) : this(key, (ushort)key.Length)
		{
			
		}

		public Slice(Slice other, ushort size)
		{
			if (other.Array != null)
				Array = other.Array;
			else
				Pointer = other.Pointer;

			Options = other.Options;
			Size = size;
			KeyLength = size;
		}

		public Slice(byte[] key, ushort size)
		{
			if (key == null) throw new ArgumentNullException("key");
			Size = size;
			KeyLength = size;
			Options = SliceOptions.Key;
			Pointer = null;
			Array = key;
		}

		public Slice(NodeHeader* node)
		{
			Options = SliceOptions.Key;
			Set(node);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != GetType()) return false;
			return Equals((Slice)obj);
		}

		public override int GetHashCode()
		{
			if (Array != null)
				return ComputeHashArray();
			return ComputeHashPointer();
		}

		private int ComputeHashPointer()
		{
			unchecked
			{
				const int p = 16777619;
				int hash = (int)2166136261;

				for (int i = 0; i < Size; i++)
					hash = (hash ^ Pointer[i]) * p;

				hash += hash << 13;
				hash ^= hash >> 7;
				hash += hash << 3;
				hash ^= hash >> 17;
				hash += hash << 5;
				return hash;
			}
		}

		private int ComputeHashArray()
		{
			unchecked
			{
				const int p = 16777619;
				int hash = (int)2166136261;

				for (int i = 0; i < Size; i++)
					hash = (hash ^ Array[i]) * p;

				hash += hash << 13;
				hash ^= hash >> 7;
				hash += hash << 3;
				hash ^= hash >> 17;
				hash += hash << 5;
				return hash;
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

		protected override int CompareData(MemorySlice other, ushort size)
		{
			var otherSlice =  other as Slice;

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
								return MemoryUtils.Compare(a, b, size);
							}
						}
						return MemoryUtils.Compare(a, otherSlice.Pointer, size);
					}
				}

				if (otherSlice.Array != null)
				{
					fixed (byte* b = otherSlice.Array)
					{
						return MemoryUtils.Compare(Pointer, b, size);
					}
				}

				return MemoryUtils.Compare(Pointer, otherSlice.Pointer, size);
			}

			var prefixedSlice = other as PrefixedSlice;

			if (prefixedSlice != null)
				return SliceComparisonMethods.Compare(this, prefixedSlice, MemoryUtils.MemoryComparerInstance, size);

			throw new NotSupportedException("Cannot compare because of unknown slice type: " + other.GetType());
		}

		protected override int CompareData(MemorySlice other, SliceComparer cmp, ushort size)
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
				return SliceComparisonMethods.Compare(this, prefixedSlice, cmp, size);

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
		public override void Set(NodeHeader* node)
		{
			Pointer = (byte*) node + Constants.NodeHeaderSize;
			Size = node->KeySize;
			KeyLength = node->KeySize;
			Array = null;
		}
	}
}