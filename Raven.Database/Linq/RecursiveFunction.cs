using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Linq;
using Raven.Database.Indexing;

namespace Raven.Database.Linq
{
	public class RecursiveFunction
	{
		private readonly object item;
		private readonly Func<object, object> func;
		readonly List<object> resultsOrdered = new List<object>();
		readonly HashSet<object> results = new HashSet<object>();
		readonly Queue<object> queue = new Queue<object>();

		public RecursiveFunction(object item, Func<object, object> func)
		{
			this.item = item;
			this.func = func;
		}

		public IEnumerable<object> Execute()
		{
			if (item == null)
				return new DynamicList(Enumerable.Empty<object>());

			var current = NullIfEmptyEnumerable(func(item));
			if (current == null)
				return new DynamicList(new[] { item });

			queue.Enqueue(item);
			while (queue.Count > 0)
			{
				current = queue.Dequeue();

				var list = current as IEnumerable<object>;
				if (list != null && AnonymousObjectToLuceneDocumentConverter.ShouldTreatAsEnumerable(current))
				{
					foreach (var o in list)
					{
						AddItem(o);
					}
				}
				else
				{
					AddItem(current);
				}
			}

			return new DynamicList(resultsOrdered.ToArray());
		}

		private void AddItem(object current)
		{
			if (results.Add(current) == false)
				return;

			resultsOrdered.Add(current);
			var result = NullIfEmptyEnumerable(func(current));
			if (result != null)
				queue.Enqueue(result);
		}

		private static object NullIfEmptyEnumerable(object item)
		{
			var enumerable = item as IEnumerable<object>;
			if (enumerable == null ||
				AnonymousObjectToLuceneDocumentConverter.ShouldTreatAsEnumerable(item) == false)
				return item;

			var enumerator = enumerable.GetEnumerator();
			return enumerator.MoveNext() == false ? null : new DynamicList(Yield(enumerator));
		}

		private static IEnumerable<object> Yield(IEnumerator<object> enumerator)
		{
			do
			{
				yield return enumerator.Current;
			} while (enumerator.MoveNext());
		}
	}
}