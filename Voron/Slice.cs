using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Voron.Impl;
using Voron.Trees;

namespace Voron
{
	public unsafe class Slice
	{
		public static Slice AfterAllKeys = new Slice(SliceOptions.AfterAllKeys);
		public static Slice BeforeAllKeys = new Slice(SliceOptions.BeforeAllKeys);
	    public static Slice Empty = new Slice(new byte[0]);

		private ushort _pointerSize;
		public SliceOptions Options;
		private byte[] _array;
		private byte* _pointer;

		public ushort Size
		{
			get { return (ushort)(_array == null ? _pointerSize : _array.Length); }
		}

		public void Set(byte* p, ushort size)
		{
			_pointer = p;
			_pointerSize = size;
		}

		public Slice(SliceOptions options)
		{
			Options = options;
			_pointer = null;
			_array = null;
			_pointerSize = 0;
		}

		public Slice(byte* key, ushort size)
		{
			_pointerSize = size;
			Options = SliceOptions.Key;
			_array = null;
			_pointer = key;
		}

		public Slice(byte[] key)
		{
			if (key == null) throw new ArgumentNullException("key");
			_pointerSize = 0;
			Options = SliceOptions.Key;
			_pointer = null;
			_array = key;
		}

		public Slice(NodeHeader* node)
		{
			Options = SliceOptions.Key;
			Set(node);
		}

		protected bool Equals(Slice other)
		{
			return Compare(other, NativeMethods.memcmp) == 0;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != GetType()) return false;
			return Equals((Slice) obj);
		}

		public override int GetHashCode()
		{
			throw new NotImplementedException();
		}

		public override string ToString()
		{
			// this is used for debug purposes only
			if (Options != SliceOptions.Key)
				return Options.ToString();

			return _array != null
		               ? Encoding.UTF8.GetString(_array)
		               : Marshal.PtrToStringAnsi(new IntPtr(_pointer), _pointerSize);
		}

		public int Compare(Slice other, SliceComparer cmp)
		{
			Debug.Assert(Options == SliceOptions.Key);
			var r = CompareData(other, cmp, Math.Min(Size, other.Size));
			if (r != 0)
				return r;
			return Size - other.Size;
		}

		public bool StartsWith(Slice other, SliceComparer cmp)
		{
			if (Size < other.Size)
				return false;
			return CompareData(other, cmp, other.Size) == 0;
		}

		private int CompareData(Slice other, SliceComparer cmp, ushort size)
		{
			if (_array != null)
			{
				fixed (byte* a = _array)
				{
					if (other._array != null)
					{
						fixed (byte* b = other._array)
						{
							return cmp(a, b, size);
						}
					}
					return cmp(a, other._pointer, size);
				}
			}
			if (other._array != null)
			{
				fixed (byte* b = other._array)
				{
					return cmp(_pointer, b, size);
				}
			}
			return cmp(_pointer, other._pointer, size);
		}

		public static implicit operator Slice(string s)
		{
			return new Slice(Encoding.UTF8.GetBytes(s));
		}

		public void CopyTo(byte* dest)
		{
			if (_array == null)
			{
				NativeMethods.memcpy(dest, _pointer, _pointerSize);
				return;
			}
			fixed (byte* a = _array)
			{
				NativeMethods.memcpy(dest, a, _array.Length);
			}
		}

		public void Set(NodeHeader* node)
		{
			Set((byte*)node + Constants.NodeHeaderSize, node->KeySize);
		}


	    public Slice Clone()
	    {
            var buffer = new byte[Size];
            if (_array == null)
            {
                fixed (byte* dest = buffer)
                {
                    NativeMethods.memcpy(dest, _pointer, _pointerSize);
                }
            }
            else
            {
                Buffer.BlockCopy(_array, 0, buffer, 0, Size);
            }
            return new Slice(buffer);
	    }
	}
}