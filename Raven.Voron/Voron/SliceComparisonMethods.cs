namespace Voron
{
	using System;
	using System.Runtime.CompilerServices;

	public unsafe delegate int SliceComparer(byte* a, byte* b, int size);

	public unsafe static class SliceComparisonMethods
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static int Compare(Slice x, PrefixedSlice y, SliceComparer cmp, ushort size)
		{
			fixed (byte* p1 = x.Array)
			fixed (byte* p2 = y.NonPrefixedData.Array)
			{
				var xPtr = p1 != null ? p1 : x.Pointer;
				var yPtr = p2 != null ? p2 : y.NonPrefixedData.Pointer;

				if (y.Header.PrefixId == PrefixedSlice.NonPrefixedId)
					return Compare(null, 0, null, 0, xPtr, x.KeyLength, yPtr, y.Header.NonPrefixedDataSize, cmp, size);

				if (x.PrefixComparisonCache == null)
					return Compare(null, 0, y.PrefixValue, y.Header.PrefixUsage, xPtr, x.KeyLength, yPtr, y.Header.NonPrefixedDataSize, cmp, size);

				var prefixBytesToCompare = Math.Min(y.Header.PrefixUsage, x.KeyLength);

				int r;

				if (x.PrefixComparisonCache.TryGetCachedResult(y.Header.PrefixId, y.Prefix.PageNumber, prefixBytesToCompare, out r) == false)
				{
					r = Compare(null, 0, y.PrefixValue, y.Header.PrefixUsage, xPtr, x.KeyLength, null, 0, cmp,
						prefixBytesToCompare);

					x.PrefixComparisonCache.SetPrefixComparisonResult(y.Header.PrefixId, y.Prefix.PageNumber, prefixBytesToCompare, r);
				}

				if (r != 0)
					return r;

				size -= prefixBytesToCompare;

				return Compare(null, 0, null, 0, xPtr + prefixBytesToCompare, (ushort)(x.KeyLength - prefixBytesToCompare), yPtr, y.Header.NonPrefixedDataSize, cmp, size);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static int Compare(PrefixedSlice x, PrefixedSlice y, SliceComparer cmp, ushort size)
		{
			fixed (byte* p1 = x.NonPrefixedData.Array)
			fixed (byte* p2 = y.NonPrefixedData.Array)
			{
				var xPtr = p1 != null ? p1 : x.NonPrefixedData.Pointer;
				var yPtr = p2 != null ? p2 : y.NonPrefixedData.Pointer;

				return Compare(x.PrefixValue, x.Header.PrefixUsage, y.PrefixValue, y.Header.PrefixUsage, xPtr, x.Header.NonPrefixedDataSize,
					yPtr, y.Header.NonPrefixedDataSize, cmp, size);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static int Compare(byte* prefix_x, ushort prefix_x_len, byte* prefix_y, ushort prefix_y_len, byte* x, ushort x_len, byte* y,
		   ushort y_len, SliceComparer cmp, ushort size)
		{
			if (size == 0) // empty slice before all keys
				return 0;

			if (prefix_x_len == 0 && prefix_y_len == 0)
				return cmp(x, y, size);

			ushort toCompare;

			if (prefix_x_len == 0)
			{
				toCompare = Math.Min(prefix_y_len, size);

				var r = cmp(x, prefix_y, toCompare);

				if (r != 0)
					return r;

				size -= toCompare;

				return cmp(x + prefix_y_len, y, size);
			}

			if (prefix_y_len == 0)
			{
				toCompare = Math.Min(prefix_x_len, size);

				var r = cmp(prefix_x, y, toCompare);

				if (r != 0)
					return r;

				size -= toCompare;

				return cmp(x, y + prefix_x_len, size);
			}

			if (prefix_x_len > prefix_y_len)
			{
				var r = cmp(prefix_x, prefix_y, prefix_y_len);

				if (r != 0)
					return r;

				size -= prefix_y_len;

				toCompare = Math.Min((ushort)(prefix_x_len - prefix_y_len), size);

				r = cmp(prefix_x + prefix_y_len, y, toCompare);

				if (r != 0)
					return r;

				size -= toCompare;

				return cmp(x, y + toCompare, size);
			}
			else
			{
				var r = cmp(prefix_x, prefix_y, prefix_x_len);

				if (r != 0)
					return r;

				size -= prefix_x_len;

				toCompare = Math.Min((ushort)(prefix_y_len - prefix_x_len), size);

				r = cmp(x, prefix_y + prefix_x_len, toCompare);

				if (r != 0)
					return r;

				size -= toCompare;

				return cmp(x + toCompare, y, size);
			}
		}
	}
}