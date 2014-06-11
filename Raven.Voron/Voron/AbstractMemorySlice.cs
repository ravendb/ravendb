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
	public unsafe abstract class AbstractMemorySlice : IMemorySlice
	{
		public abstract ushort Size { get; }
		public abstract ushort KeyLength { get; }
		public SliceOptions Options { get; protected set; }
		public PrefixComparisonCache PrefixComparisonCache = new PrefixComparisonCache();

		public abstract void CopyTo(byte* dest);
		public abstract Slice ToSlice();
		public abstract Slice Skip(ushort bytesToSkip);

		protected abstract int CompareData(IMemorySlice other, SliceComparer cmp, ushort size);

		public bool Equals(IMemorySlice other)
		{
			return Compare(other) == 0;
		}

		public int Compare(IMemorySlice other)
		{
			Debug.Assert(Options == SliceOptions.Key);
			Debug.Assert(other.Options == SliceOptions.Key);

			var r = CompareData(other, SliceComparisonMethods.NativeMemCmpInstance, Math.Min(KeyLength, other.KeyLength));
			if (r != 0)
				return r;

			return KeyLength - other.KeyLength;
		}

		public bool StartsWith(IMemorySlice other)
		{
			if (KeyLength < other.KeyLength)
				return false;
			return CompareData(other, SliceComparisonMethods.NativeMemCmpInstance, other.KeyLength) == 0;
		}

		public ushort FindPrefixSize(IMemorySlice other)
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