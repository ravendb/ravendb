using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Linq;

namespace Raven.Database.Linq.PrivateExtensions
{
	public static class DynamicEnumerable
	{
		public static dynamic First<TSource>(IEnumerable<TSource> source)
		{
			return FirstOrDefault(source);
		}

		public static dynamic First<TSource>(IEnumerable<TSource> source, Func<TSource, bool> predicate)
		{
			return FirstOrDefault(source, predicate);
		}

		public static dynamic FirstOrDefault<TSource>(IEnumerable<TSource> source)
		{
			if (source == null) return new DynamicNullObject();

			var result = source.FirstOrDefault();
			if (ReferenceEquals(result, null))
				return new DynamicNullObject();
			return result;
		}

		public static dynamic FirstOrDefault<TSource>(IEnumerable<TSource> source, Func<TSource, bool> predicate)
		{
			if (source == null) return new DynamicNullObject();
			if (predicate == null) return new DynamicNullObject();

			var result = source.FirstOrDefault(predicate);
			if (ReferenceEquals(result, null))
				return new DynamicNullObject();
			return result;
		}

		public static dynamic Last<TSource>(IEnumerable<TSource> source)
		{
			return LastOrDefault(source);

		}

		public static dynamic Last<TSource>(IEnumerable<TSource> source, Func<TSource, bool> predicate)
		{
			return LastOrDefault(source, predicate);
		}

		public static dynamic LastOrDefault<TSource>(IEnumerable<TSource> source)
		{
			if (source == null) return new DynamicNullObject();

			var result = source.LastOrDefault();
			if (ReferenceEquals(result, null))
				return new DynamicNullObject();
			return result;
		}

		public static dynamic LastOrDefault<TSource>(IEnumerable<TSource> source, Func<TSource, bool> predicate)
		{
			if (source == null) return new DynamicNullObject();
			if (predicate == null) return new DynamicNullObject();

			var result = source.LastOrDefault(predicate);
			if (ReferenceEquals(result, null))
				return new DynamicNullObject();
			return result;
		}

		public static dynamic Single<TSource>(IEnumerable<TSource> source)
		{
			return SingleOrDefault(source);
		}

		public static dynamic Single<TSource>(IEnumerable<TSource> source, Func<TSource, bool> predicate)
		{
			return SingleOrDefault(source, predicate);
		}

		public static dynamic SingleOrDefault<TSource>(IEnumerable<TSource> source)
		{
			if (source == null) return new DynamicNullObject();

			var result = source.SingleOrDefault();
			if (ReferenceEquals(result, null))
				return new DynamicNullObject();
			return result;
		}

		public static dynamic SingleOrDefault<TSource>(IEnumerable<TSource> source, Func<TSource, bool> predicate)
		{
			if (source == null) return new DynamicNullObject();
			if (predicate == null) return new DynamicNullObject();

			var result = source.SingleOrDefault(predicate);
			if (ReferenceEquals(result, null))
				return new DynamicNullObject();
			return result;
		}

		public static dynamic ElementAt<TSource>(IEnumerable<TSource> source, int index)
		{
			return ElementAtOrDefault(source, index);

		}

		public static dynamic ElementAtOrDefault<TSource>(IEnumerable<TSource> source, int index)
		{
			if (source == null) return new DynamicNullObject();
			if (index < 0) return new DynamicNullObject();

			var result = source.ElementAtOrDefault(index);
			if (ReferenceEquals(result, null))
				return new DynamicNullObject();
			return result;
		}
	}
}