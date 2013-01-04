using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Studio.Infrastructure
{
	public class KeysComparer<T> : IEqualityComparer<T>
	{
		private readonly List<Func<T, object>> keysExtractor;

		public KeysComparer(params Func<T, object>[] keysExtractor)
		{
			this.keysExtractor = keysExtractor.ToList();
		}

		public bool Equals(T x, T y)
		{
			foreach (var keyExtractor in keysExtractor)
			{
				var xKey = keyExtractor(x);
				var yKey = keyExtractor(y);

				if (Equals(xKey ?? x, yKey ?? y) == false)
					return false;
			}
			return true;
		}

		public int GetHashCode(T obj)
		{
			int result = 0;
			foreach (var keyExtractor in keysExtractor)
			{
				var extractor = keyExtractor(obj);
				result = result ^ extractor.GetHashCode() * 397;
			}
			return result;
		}

		public void Add(Func<T, object> extractor)
		{
			keysExtractor.Add(extractor);
		}
	}
}