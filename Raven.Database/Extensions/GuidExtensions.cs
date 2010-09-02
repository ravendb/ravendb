using System;

namespace Raven.Database.Extensions
{
	public static class GuidExtensions
	{
		private static readonly int[] normalizations = new int[] { 10, 11, 12, 13, 14, 15, 8, 9, 6, 7, 4, 5, 0, 1, 2, 3 };

		public static Guid TransfromToGuidWithProperSorting(this byte[] bytes)
		{
			return new Guid(bytes);
		}

		public static byte[] TransformToValueForEsentSorting(this Guid guid)
		{
			return guid.ToByteArray();
		}
	}
}