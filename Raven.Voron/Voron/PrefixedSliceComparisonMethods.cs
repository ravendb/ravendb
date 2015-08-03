namespace Voron
{
	using System;
	using System.Runtime.CompilerServices;

	public unsafe delegate int PrefixedSliceComparer(byte* a, byte* b, int size);

	public unsafe static class PrefixedSliceComparisonMethods
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static int Compare(Slice x, PrefixedSlice y, PrefixedSliceComparer cmp, ushort size)
		{
			fixed (byte* p1 = x.Array)
			fixed (byte* p2 = y.NonPrefixedData.Array)
			{
				var xPtr = p1 != null ? p1 : x.Pointer;
				var yPtr = p2 != null ? p2 : y.NonPrefixedData.Pointer;

				if (y.Header.PrefixId == PrefixedSlice.NonPrefixedId)
					return Compare(null, 0, null, 0, xPtr, x.KeyLength, yPtr, y.Header.NonPrefixedDataSize, cmp, size);

				if (x.PrefixComparisonCache == null)
				{
					if(y.Prefix == null)
						return Compare(null, 0, null, 0, xPtr, x.KeyLength, yPtr, y.Header.NonPrefixedDataSize, cmp, size);
					else if (y.Prefix.Value == null)
						return Compare(null, 0, y.Prefix.ValuePtr, y.Header.PrefixUsage, xPtr, x.KeyLength, yPtr, y.Header.NonPrefixedDataSize, cmp, size);
					else
					{
						fixed (byte* prefixVal = y.Prefix.Value)
							return Compare(null, 0, prefixVal, y.Header.PrefixUsage, xPtr, x.KeyLength, yPtr, y.Header.NonPrefixedDataSize, cmp, size);
					}
				}

				var prefixBytesToCompare = Math.Min(y.Header.PrefixUsage, x.KeyLength);

				int r;

				if (x.PrefixComparisonCache.TryGetCachedResult(y.Header.PrefixId, y.Prefix.PageNumber, prefixBytesToCompare, out r) == false)
				{
					if (y.Prefix == null)
						r = Compare(null, 0, null, 0, xPtr, x.KeyLength, null, 0, cmp,
							prefixBytesToCompare);

					else if (y.Prefix.Value == null)
						r = Compare(null, 0, y.Prefix.ValuePtr, y.Header.PrefixUsage, xPtr, x.KeyLength, null, 0, cmp,
							prefixBytesToCompare);
					else
					{
						fixed (byte* prefixVal = y.Prefix.Value)
							r = Compare(null, 0, prefixVal, y.Header.PrefixUsage, xPtr, x.KeyLength, null, 0, cmp,
								prefixBytesToCompare);
					}

					x.PrefixComparisonCache.SetPrefixComparisonResult(y.Header.PrefixId, y.Prefix.PageNumber, prefixBytesToCompare, r);
				}

				if (r != 0)
					return r;

				size -= prefixBytesToCompare;

				return Compare(null, 0, null, 0, xPtr + prefixBytesToCompare, (ushort)(x.KeyLength - prefixBytesToCompare), yPtr, y.Header.NonPrefixedDataSize, cmp, size);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static int Compare(PrefixedSlice x, PrefixedSlice y, PrefixedSliceComparer cmp, ushort size)
		{
			fixed (byte* p1 = x.NonPrefixedData.Array)
			fixed (byte* p2 = y.NonPrefixedData.Array)
			{
				var xPtr = p1 != null ? p1 : x.NonPrefixedData.Pointer;
				var yPtr = p2 != null ? p2 : y.NonPrefixedData.Pointer;

				byte* xPre = null;
				byte* yPre = null;

				fixed (byte* pre1 = x.Prefix != null ? x.Prefix.Value : null)
				fixed (byte* pre2 = y.Prefix != null ? y.Prefix.Value : null)
				{
					if(x.Prefix != null)
						xPre = pre1 != null ? pre1 : x.Prefix.ValuePtr;
					if (y.Prefix != null)
						yPre = pre2 != null ? pre2 : y.Prefix.ValuePtr;

					return Compare(xPre, x.Header.PrefixUsage, yPre, y.Header.PrefixUsage, xPtr, x.Header.NonPrefixedDataSize,
						yPtr, y.Header.NonPrefixedDataSize, cmp, size);
				}
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static int Compare(byte* prefix_x, ushort prefix_x_len, byte* prefix_y, ushort prefix_y_len, byte* x, ushort x_len, byte* y,
		   ushort y_len, PrefixedSliceComparer cmp, ushort size)
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