using System;
using System.Text;
using Voron.Impl;

namespace Voron
{
	public unsafe class Slice : MemorySlice
	{
		public static Slice AfterAllKeys = new Slice(SliceOptions.AfterAllKeys);
		public static Slice BeforeAllKeys = new Slice(SliceOptions.BeforeAllKeys);
		public static Slice Empty = new Slice(new byte[0]);

		internal readonly byte[] Array;
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

			if (Array != null)
				return Encoding.UTF8.GetString(Array,0, Size);

			return new string((sbyte*)Pointer, 0, Size, Encoding.UTF8);
		}

		public IDisposable GetPointer(out byte* ptr)
		{
			if (Array != null)
			{
				fixed (byte* a = Array)
				{
					ptr = a;
				}
			}

			ptr = Pointer;

			return null;
		}

		protected override int CompareData(MemorySlice other, SliceComparer cmp, ushort size)
		{
			var otherSlice = other as Slice;

			if (otherSlice != null)
				return SliceComparisonMethods.Compare(this, otherSlice, cmp, size);

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
				NativeMethods.memcpy(dest, Pointer, Size);
				return;
			}
			fixed (byte* a = Array)
			{
				NativeMethods.memcpy(dest, a, Size);
			}
		}

		public override Slice ToSlice()
		{
			return this;
		}

		public void CopyTo(byte[] dest)
		{
			if (Array == null)
			{
				fixed (byte* p = dest)
					NativeMethods.memcpy(p, Pointer, Size);
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
					NativeMethods.memcpy(p + offset, Pointer + from, count);
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
				NativeMethods.memcpy(dest + offset, Pointer + from, count);
				return;
			}

			fixed (byte* p = Array)
				NativeMethods.memcpy(dest + offset, p + from, count);
		}

		public Slice Clone()
		{
			var buffer = new byte[Size];
			if (Array == null)
			{
				fixed (byte* dest = buffer)
				{
					NativeMethods.memcpy(dest, Pointer, Size);
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
				return this;

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

		public override ushort FindPrefixSize(MemorySlice other)
		{
			if (PrefixComparisonCache == null) 
				return base.FindPrefixSize(other);
			
			using (PrefixComparisonCache.DisablePrefixCache())
				return base.FindPrefixSize(other);
		}
	}
}