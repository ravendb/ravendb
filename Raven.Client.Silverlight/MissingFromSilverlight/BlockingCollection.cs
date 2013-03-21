// -----------------------------------------------------------------------
//  <copyright file="BlockingCollection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;

namespace System.Collections.Concurrent
{
	public class BlockingCollection<T> where T : class
	{
		private readonly int boundedCapacity;

		private readonly IList<T> collection;

		private readonly object readLocker = new object();

		public BlockingCollection(int boundedCapacity)
		{
			this.boundedCapacity = boundedCapacity;
			this.collection = new Collection<T>();
		}

		public bool TryTake(out T item, int millisecondsTimeout)
		{
			lock (readLocker)
			{
				item = default(T);

				var result = default(T);
				var success = SpinWait.SpinUntil(() =>
				{
					result = collection.FirstOrDefault();
					return collection.Count > 0;
				}, millisecondsTimeout);

				if (success)
				{
					collection.Remove(result);
					item = result;

					return true;
				}

				return false;
			}
		}

		public void Add(T item)
		{
			collection.Add(item);
		}
	}
}