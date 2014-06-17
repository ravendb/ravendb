// -----------------------------------------------------------------------
//  <copyright file="AbstractMemorySlice.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Threading;
using Voron.Impl;

namespace Voron
{
	using System.Text;

	public unsafe abstract class MemorySlice
	{
		public ushort Size;
		public ushort KeyLength;
		public SliceOptions Options;

		public abstract void CopyTo(byte* dest);
		public abstract Slice ToSlice();
		public abstract Slice Skip(ushort bytesToSkip);

		protected abstract int CompareData(MemorySlice other, SliceComparer cmp, ushort size);

		public bool Equals(MemorySlice other)
		{
			return Compare(other) == 0;
		}

		public int Compare(MemorySlice other)
		{
			Debug.Assert(Options == SliceOptions.Key);
			Debug.Assert(other.Options == SliceOptions.Key);

			var r = CompareData(other, SliceComparisonMethods.OwnMemCmpInstane, Math.Min(KeyLength, other.KeyLength));
			if (r != 0)
				return r;

			return KeyLength - other.KeyLength;
		}

		public static implicit operator MemorySlice(string s)
		{
			return new Slice(Encoding.UTF8.GetBytes(s));
		}

		public bool StartsWith(MemorySlice other)
		{
			if (KeyLength < other.KeyLength)
				return false;
			return CompareData(other, SliceComparisonMethods.OwnMemCmpInstane, other.KeyLength) == 0;
		}

		private ushort _matchedBytes;
		private SliceComparer _matchPrefixInstance;

		public ushort FindPrefixSize(MemorySlice other)
		{
			_matchedBytes = 0;

			if (_matchPrefixInstance == null)
				_matchPrefixInstance = MatchPrefix;

			CompareData(other, _matchPrefixInstance, Math.Min(KeyLength, other.KeyLength));

			return _matchedBytes;
		}

		private int MatchPrefix(byte* lhs, byte* rhs, int size)
		{
			var n = size;

			var sizeOfUInt = Constants.SizeOfUInt;

			if (n > sizeOfUInt)
			{
				var lUintAlignment = (long)lhs % sizeOfUInt;
				var rUintAlignment = (long)rhs % sizeOfUInt;

				if (lUintAlignment != 0 && lUintAlignment == rUintAlignment)
				{
					var toAlign = sizeOfUInt - lUintAlignment;
					while (toAlign > 0)
					{
						var r = *lhs++ - *rhs++;
						if (r != 0)
							return r;
						n--;
						_matchedBytes++;

						toAlign--;
					}
				}

				uint* lp = (uint*)lhs;
				uint* rp = (uint*)rhs;

				while (n > sizeOfUInt)
				{
					if (*lp != *rp)
						break;

					lp++;
					rp++;

					n -= sizeOfUInt;
					_matchedBytes += sizeOfUInt;
				}

				lhs = (byte*)lp;
				rhs = (byte*)rp;
			}

			while (n > 0)
			{
				var r = *lhs++ - *rhs++;
				if (r != 0)
					return r;
				n--;
				_matchedBytes++;
			}

			return 0;
		}

		public virtual void PrepareForSearching()
		{
		}
	}
}