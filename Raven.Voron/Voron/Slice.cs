using System;
using System.Text;
using Voron.Impl;

namespace Voron
{
	public unsafe delegate int SliceComparer(byte* a, byte* b, int size);

	public unsafe class Slice : AbstractMemorySlice
	{
		public static Slice AfterAllKeys = new Slice(SliceOptions.AfterAllKeys);
		public static Slice BeforeAllKeys = new Slice(SliceOptions.BeforeAllKeys);
		public static Slice Empty = new Slice(new byte[0]);

		private ushort _size;
		internal readonly byte[] _array;
		internal byte* _pointer;

		public override ushort Size
		{
			get { return _size; }
		}

		public override ushort KeyLength
		{
			get { return _size; }
		}

		public Slice(SliceOptions options)
		{
			Options = options;
			_pointer = null;
			_array = null;
			_size = 0;
		}

		public Slice(byte* key, ushort size)
		{
			_size = size;
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
			_size = size;
		}

		public Slice(byte[] key, ushort size)
		{
			if (key == null) throw new ArgumentNullException("key");
			_size = size;
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

				for (int i = 0; i < _size; i++)
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

				for (int i = 0; i < _size; i++)
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
				return Encoding.UTF8.GetString(_array,0, _size);

			return new string((sbyte*)_pointer, 0, _size, Encoding.UTF8);
		}

		protected override int CompareData(IMemorySlice other, SliceComparer cmp, ushort size)
		{
			var otherSlice = other as Slice;

			if (otherSlice != null)
				return CompareSlices(otherSlice, cmp, size);

			var prefixedSlice = other as PrefixedSlice;

			if (prefixedSlice != null)
			{
				var prefixLength = Math.Min(prefixedSlice.Header.PrefixUsage, size);

				var r = prefixedSlice.ComparePrefixWithNonPrefixedData(this, cmp, 0, prefixLength);

				if (r != 0)
					return r * -1;

				// compare non prefixed data

				size -= prefixLength;

				r = prefixedSlice.CompareNonPrefixedData(0, this, prefixLength, cmp, size);

				return r * -1;
			}

			throw new NotSupportedException("Cannot compare because of unknown slice type: " + other.GetType());
		}

		internal int CompareSlices(Slice otherSlice, SliceComparer cmp, int size, int offset = 0, int otherOffset = 0)
		{
			if (_array != null)
			{
				fixed (byte* a = _array)
				{
					if (otherSlice._array != null)
					{
						fixed (byte* b = otherSlice._array)
						{
							return cmp(a + offset, b + otherOffset, size);
						}
					}
					return cmp(a + offset, otherSlice._pointer + otherOffset, size);
				}
			}
			if (otherSlice._array != null)
			{
				fixed (byte* b = otherSlice._array)
				{
					return cmp(_pointer + offset, b + otherOffset, size);
				}
			}
			return cmp(_pointer + offset, otherSlice._pointer + otherOffset, size);
		}

		public static implicit operator Slice(string s)
		{
			return new Slice(Encoding.UTF8.GetBytes(s));
		}

		public override void CopyTo(byte* dest)
		{
			if (_array == null)
			{
				NativeMethods.memcpy(dest, _pointer, _size);
				return;
			}
			fixed (byte* a = _array)
			{
				NativeMethods.memcpy(dest, a, _size);
			}
		}

		public override void CopyTo(byte[] dest)
		{
			if (_array == null)
			{
				fixed (byte* p = dest)
					NativeMethods.memcpy(p, _pointer, _size);
				return;
			}
			Buffer.BlockCopy(_array, 0, dest, 0, _size);
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
					NativeMethods.memcpy(dest, _pointer, _size);
				}
			}
			else
			{
				Buffer.BlockCopy(_array, 0, buffer, 0, Size);
			}
			return new Slice(buffer);
		}

	    public override ValueReader CreateReader()
	    {
            if(_array != null)
                return new ValueReader(_array, _size);

	        return new ValueReader(_pointer, _size);
	    }

		public override Slice Skip(ushort bytesToSkip)
		{
			if (_pointer != null)
				return new Slice(_pointer + bytesToSkip, (ushort)(_size - bytesToSkip));

			var toAllocate = _size - bytesToSkip;
			var array = new byte[toAllocate];

			Buffer.BlockCopy(_array, bytesToSkip, array, 0, toAllocate);

			return new Slice(array);
		}
	}
}