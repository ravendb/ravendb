using Raven.Abstractions.Data;

namespace Raven.Abstractions.Util
{
	public static class EtagUtil
	{
		public static Etag Increment(Etag etag, int amount)
		{
		    return etag.IncrementBy(amount);
		}

		public static long GetChangesCount(Etag etag)
		{
            return etag.Changes;
		}

		public static bool IsGreaterThan(Etag x, Etag y)
		{
			return x.CompareTo(y) > 0;
		}

		public static bool IsGreaterThanOrEqual(Etag x, Etag y)
		{
		    return x.CompareTo(y) >= 0;
		}

        public static byte[] TransformToValueForEsentSorting(this Etag etag)
        {
            return etag.ToByteArray();
        }
	}
}