using System;
using System.Collections.Generic;

namespace Raven.Database.Extensions
{
	public static class EnumerableExtensions
	{
		public static void Apply<T>(this IEnumerable<T> self, Action<T> action)
		{
			foreach (var item in self)
			{
				action(item);
			}
		}
	}
}