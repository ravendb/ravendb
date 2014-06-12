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

		internal readonly byte[] _array;
		internal byte* _pointer;

		public Slice(SliceOptions options)
		{
			Options = options;
			_pointer = null;
			_array = null;
			Size = 0;
			KeyLength = 0;
		}

		public Slice(byte* key, ushort size)
		{
			Size = size;
			KeyLength = size;
			Options = SliceOptions.Key;
			_array = null;
			_pointer = key;
		}

		public Slice(byte[] key) : this(key, (ushort)key.Length)
		{
			
		}

		public Slice(Slice other, ushort size)
		{
			if (other._array != null)
				_array = other._array;
			else
				_pointer = other._pointer;

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
			_pointer = null;
			_array = key;
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
			if (_array != null)
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
					hash = (hash ^ _pointer[i]) * p;

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
					hash = (hash ^ _array[i]) * p;

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

			if (_array != null)
				return Encoding.UTF8.GetString(_array,0, Size);

			return new string((sbyte*)_pointer, 0, Size, Encoding.UTF8);
		}

		public IDisposable GetPointer(out byte* ptr)
		{
			if (_array != null)
			{
				fixed (byte* a = _array)
				{
					ptr = a;
				}
			}

			ptr = _pointer;

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
			if (_array == null)
			{
				NativeMethods.memcpy(dest, _pointer, Size);
				return;
			}
			fixed (byte* a = _array)
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
			if (_array == null)
			{
				fixed (byte* p = dest)
					NativeMethods.memcpy(p, _pointer, Size);
				return;
			}
			Buffer.BlockCopy(_array, 0, dest, 0, Size);
		}

		public void CopyTo(int from, byte[] dest, int offset, int count)
		{
			if (from + count > Size)
				throw new ArgumentOutOfRangeException("from", "Cannot copy data after the end of the slice");
			if(offset + count > dest.Length)
				throw new ArgumentOutOfRangeException("from", "Cannot copy data after the end of the buffer" +
				                                              "");
			if (_array == null)
			{
				fixed (byte* p = dest)
					NativeMethods.memcpy(p + offset, _pointer + from, count);
				return;
			}
			Buffer.BlockCopy(_array, from, dest, offset, count);
		}

		public void CopyTo(int from, byte* dest, int offset, int count)
		{
			if (from + count > Size)
				throw new ArgumentOutOfRangeException("from", "Cannot copy data after the end of the slice");

			if (_array == null)
			{
				NativeMethods.memcpy(dest + offset, _pointer + from, count);
				return;
			}

			fixed (byte* p = _array)
				NativeMethods.memcpy(dest + offset, p + from, count);
		}

		public Slice Clone()
		{
			var buffer = new byte[Size];
			if (_array == null)
			{
				fixed (byte* dest = buffer)
				{
					NativeMethods.memcpy(dest, _pointer, Size);
				}
			}
			else
			{
				Buffer.BlockCopy(_array, 0, buffer, 0, Size);
			}

			return new Slice(buffer);
		}

	    public ValueReader CreateReader()
	    {
            if(_array != null)
                return new ValueReader(_array, Size);

	        return new ValueReader(_pointer, Size);
	    }

		public override Slice Skip(ushort bytesToSkip)
		{
			if (bytesToSkip == 0)
				return this;

			if (_pointer != null)
				return new Slice(_pointer + bytesToSkip, (ushort)(Size - bytesToSkip));

			var toAllocate = Size - bytesToSkip;
			var array = new byte[toAllocate];

			Buffer.BlockCopy(_array, bytesToSkip, array, 0, toAllocate);

			return new Slice(array);
		}

		public PrefixComparisonCache PrefixComparisonCache = new PrefixComparisonCache();

		public override ushort FindPrefixSize(MemorySlice other)
		{
			using (PrefixComparisonCache.DisablePrefixCache())
				return base.FindPrefixSize(other);
		}
	}
}