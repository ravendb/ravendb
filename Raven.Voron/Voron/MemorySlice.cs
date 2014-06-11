// -----------------------------------------------------------------------
//  <copyright file="AbstractMemorySlice.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;

namespace Voron
{
	using System.Text;

	public unsafe abstract class MemorySlice
	{
		public ushort Size;
		public ushort KeyLength;
		public SliceOptions Options;
		public PrefixComparisonCache PrefixComparisonCache = new PrefixComparisonCache();

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

			var r = CompareData(other, SliceComparisonMethods.NativeMemCmpInstance, Math.Min(KeyLength, other.KeyLength));
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
			return CompareData(other, SliceComparisonMethods.NativeMemCmpInstance, other.KeyLength) == 0;
		}

		public ushort FindPrefixSize(MemorySlice other)
		{
			var maxPrefixLength = Math.Min(KeyLength, other.KeyLength);

			using (PrefixComparisonCache != null ? PrefixComparisonCache.DisablePrefixCache() : null)
			{
				var slicePrefixMatcher = new SlicePrefixMatcher(maxPrefixLength);
				CompareData(other, slicePrefixMatcher.MatchPrefix, maxPrefixLength);

				return slicePrefixMatcher.MatchedBytes;
			}
		}

		private class SlicePrefixMatcher
		{
			private readonly int _maxPrefixLength;

			public SlicePrefixMatcher(int maxPrefixLength)
			{
				_maxPrefixLength = maxPrefixLength;
				MatchedBytes = 0;
			}

			public ushort MatchedBytes { get; private set; }

			public int MatchPrefix(byte* a, byte* b, int size)
			{
				for (var i = 0; i < Math.Min(_maxPrefixLength, size); i++)
				{
					if (*a == *b)
						MatchedBytes++;
					else
						return *a > *b ? 1 : -1;

					a++;
					b++;
				}

				return 0;
			}
		}
	}
}