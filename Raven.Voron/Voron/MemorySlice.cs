// -----------------------------------------------------------------------
//  <copyright file="AbstractMemorySlice.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
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

		public virtual ushort FindPrefixSize(MemorySlice other)
		{
			var maxPrefixLength = Math.Min(KeyLength, other.KeyLength);

			SlicePrefixMatcher.Init(maxPrefixLength);
			CompareData(other, SlicePrefixMatcher.MatchPrefixMethodInstance, maxPrefixLength);

			return SlicePrefixMatcher.MatchedBytes;
		}

		private static class SlicePrefixMatcher
		{
			private static int _maxPrefixLength;

			public static readonly SliceComparer MatchPrefixMethodInstance = MatchPrefix;
			public static ushort MatchedBytes;

			public static void Init(int maxPrefixLength)
			{
				_maxPrefixLength = maxPrefixLength;
				MatchedBytes = 0;
			}

			private static int MatchPrefix(byte* a, byte* b, int size)
			{
				var n = Math.Min(_maxPrefixLength, size);

				uint* lp = (uint*)a;
				uint* rp = (uint*)b;

				while (n > Constants.SizeOfUInt)
				{
					if (*lp != *rp)
						break;

					lp++;
					rp++;

					n -= Constants.SizeOfUInt;
					MatchedBytes += Constants.SizeOfUInt;
				}

				a = (byte*)lp;
				b = (byte*)rp;

				while (n > 0)
				{
					var r = *a++ - *b++;
					if (r != 0)
						return r;
					n--;
					MatchedBytes++;
				}

				return 0;
			}
		}

		public virtual void PrepareForSearching()
		{
		}
	}
}