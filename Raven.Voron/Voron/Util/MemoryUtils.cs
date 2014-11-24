namespace Voron.Util
{
	using System.Runtime.CompilerServices;
	using Impl;

	public unsafe static class MemoryUtils
	{
		public static SliceComparer MemoryComparerInstance = Compare;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static int Compare(byte* lhs, byte* rhs, int n)
		{
			if (n == 0)
				return 0;

			var sizeOfUInt = Constants.SizeOfUInt;

			if (n >= sizeOfUInt)
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
			}

			return 0;
		}
	}
}