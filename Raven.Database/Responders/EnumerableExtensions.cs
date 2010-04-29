using System.Collections.Generic;

namespace Raven.Database.Responders
{
	public static class EnumerableExtensions
	{
		public static IEnumerable<T> EmptyIfNull<T>(this IEnumerable<T> self)
		{
			if (self == null)
				return new T[0];
			return self;
		}
	}
}