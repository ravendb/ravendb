using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;

namespace Nevar
{
	public unsafe delegate int SliceComparer(byte* a, byte* b, int size);

	public enum SliceOptions : byte
	{
		Key = 0,
		BeforeAllKeys = 1,
		AfterAllKeys = 2
	}

	public unsafe struct Slice
	{
		private ushort _pointerSize;
		public SliceOptions Options;
		private readonly byte[] _array;
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
			Debug.Assert(size > 0 && size < Constants.MaxKeySize);
			_pointerSize = size;
			Options = SliceOptions.Key;
			_array = null;
			_pointer = key;
		}

		public Slice(byte[] key)
		{
			if (key == null) throw new ArgumentNullException("key");
			if (key.Length > Constants.MaxKeySize)
				throw new ArgumentException(
					"Key size is too big, must be at most " + Constants.MaxKeySize + " bytes, but was " + key.Length, "key");

			_pointerSize = 0;
			Options = SliceOptions.Key;
			_pointer = null;
			_array = key;
		}

		public override string ToString()
		{
			// this is used for debug purposes only

			if (Options != SliceOptions.Key)
				return Options.ToString();

			if (_array != null) // an array
			{
				return Encoding.UTF8.GetString(_array);
			}
			return Marshal.PtrToStringAnsi(new IntPtr(_pointer), _pointerSize);
		}

		public int Compare(Slice other, SliceComparer cmp)
		{
			Debug.Assert(Options == SliceOptions.Key);
			if (_array != null)
			{
				fixed (byte* a = _array)
				{
					if (other._array != null)
					{
						fixed (byte* b = other._array)
						{
							return cmp(a, b, Math.Min(_array.Length, other._array.Length));
						}
					}
					return cmp(a, other._pointer, Math.Min(_array.Length, other._pointerSize));
				}
			}
			if (_array != null)
			{
				fixed (byte* b = other._array)
				{
					return cmp(_pointer, b, Math.Min(_pointerSize, other._array.Length));
				}
			}
			return cmp(_pointer, other._pointer, Math.Min(_pointerSize, other._pointerSize));
		}

		public static implicit operator Slice(string s)
		{
			return new Slice(Encoding.UTF8.GetBytes(s));
		}

		public void CopyTo(byte* dest)
		{
			if (_array == null)
			{
				NativeMethods.MemCpy(dest, _pointer, _pointerSize);
				return;
			}
			fixed (byte* a = _array)
			{
				NativeMethods.MemCpy(dest, a, _array.Length);
			}
		}
	}
}