using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public static class EnumerableExtension
	{
		public static void ApplyIfNotNull<T>(this IEnumerable<T> self, Action<T> action)
		{
			if (self == null)
				return;
			foreach (var item in self)
			{
				action(item);
			}
		}

        public static IEnumerable<T> EmptyIfNull<T>(this IEnumerable<T> self)
        {
            if (self == null)
                return new T[0];
            return self;
        }
	}
}