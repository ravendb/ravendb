using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Extensions
{
	public static class ListExtensions
	{
		public static void ForEach<T>(this IEnumerable<T> source, Action<T> action)
		{
			foreach (T element in source)
			{
				action(element);
			}
		}

        public static void AddRange<T>(this IList<T> list, IEnumerable<T> items)
        {
            foreach (var item in items)
            {
                list.Add(item);
            }
        }

        public static HashSet<T> ToHashSet<T>(this IEnumerable<T> items)
        {
            return new HashSet<T>(items);
        } 
	}
}
