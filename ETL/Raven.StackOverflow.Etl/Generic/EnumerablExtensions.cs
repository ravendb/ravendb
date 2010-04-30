using System.Collections.Generic;

namespace Raven.StackOverflow.Etl.Generic
{
	public static class EnumerablExtensions
	{
		public static IEnumerable<IEnumerable<T>> Partition<T>(this IEnumerable<T> self, int size)
		{
			var items = new List<T>();
			foreach (var item in self)
			{
				items.Add(item);
				if (items.Count < size)
					continue;
				yield return items;
				items.Clear();
			}
			yield return items;
		}
	}
}