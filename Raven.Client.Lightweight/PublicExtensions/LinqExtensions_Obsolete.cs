using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Raven.Client
{
	using System;
	using System.Collections.Generic;
	using Raven.Client.Linq;

	///<summary>
	/// Extensions to the linq syntax
	///</summary>
	public static partial class LinqExtensions
	{
#if SILVERLIGHT
		/// <summary>
		///   This function exists solely to forbid calling ToList() on a queryable in Silverlight.
		/// </summary>
		[Obsolete(
			"You cannot execute a query synchronously from the Silverlight client. Instead, use queryable.ToListAsync().", true)]
		public static IList<T> ToList<T>(this IRavenQueryable<T> source)
		{
			throw new NotSupportedException();
		}

		/// <summary>
		///   This function exists solely to forbid calling ToArray() on a queryable in Silverlight.
		/// </summary>
		[Obsolete(
			"You cannot execute a query synchronously from the Silverlight client. Instead, use queryable.ToListAsync().", true)]
		public static T[] ToArray<T>(this IRavenQueryable<T> source)
		{
			throw new NotSupportedException();
		}

		/// <summary>
		///   This function exists solely to forbid calling ToDictionary() on a queryable in Silverlight.
		/// </summary>
		[Obsolete(
			"You cannot execute a query synchronously from the Silverlight client. Instead, use queryable.ToListAsync().", true)]
		public static Dictionary<TKey, TSource> ToDictionary<TSource, TKey>(this IEnumerable<TSource> source,
		                                                                    Func<TSource, TKey> keySelector)
		{
			throw new NotSupportedException();
		}

		/// <summary>
		///   This function exists solely to forbid calling ToDictionary() on a queryable in Silverlight.
		/// </summary>
		[Obsolete(
			"You cannot execute a query synchronously from the Silverlight client. Instead, use queryable.ToListAsync().", true)]
		public static Dictionary<TKey, TSource> ToDictionary<TSource, TKey>(this IEnumerable<TSource> source,
		                                                                    Func<TSource, TKey> keySelector,
		                                                                    IEqualityComparer<TKey> comparer)
		{
			throw new NotSupportedException();
		}

		/// <summary>
		///   This function exists solely to forbid calling ToDictionary() on a queryable in Silverlight.
		/// </summary>
		[Obsolete(
			"You cannot execute a query synchronously from the Silverlight client. Instead, use queryable.ToListAsync().", true)]
		public static Dictionary<TKey, TElement> ToDictionary<TSource, TKey, TElement>(this IEnumerable<TSource> source,
		                                                                               Func<TSource, TKey> keySelector,
		                                                                               Func<TSource, TElement> elementSelector)
		{
			throw new NotSupportedException();
		}

		/// <summary>
		///   This function exists solely to forbid calling ToDictionary() on a queryable in Silverlight.
		/// </summary>
		[Obsolete(
			"You cannot execute a query synchronously from the Silverlight client. Instead, use queryable.ToListAsync().", true)]
		public static Dictionary<TKey, TElement> ToDictionary<TSource, TKey, TElement>(this IEnumerable<TSource> source,
		                                                                               Func<TSource, TKey> keySelector,
		                                                                               Func<TSource, TElement> elementSelector,
		                                                                               IEqualityComparer<TKey> comparer)
		{
			throw new NotSupportedException();
		}

#endif
	}
}