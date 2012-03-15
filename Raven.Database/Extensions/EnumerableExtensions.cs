//-----------------------------------------------------------------------
// <copyright file="EnumerableExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

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

		public static IEnumerable<T> EmptyIfNull<T>(this IEnumerable<T> self)
		{
			if (self == null)
				return new T[0];
			return self;
		}

		public static void ApplyAndIgnoreAllErrors<T>(this IEnumerable<T> self, Action<Exception> errorAction, Action<T> action)
		{
			foreach (var item in self)
			{
				try
				{
					action(item);
				}
				catch (Exception e)
				{
					errorAction(e);
				}
			}
		}

		//Sample based on code from http://edulinq.googlecode.com/hg/posts/16-Intersect.html
		//We've effectively got a limited set of elements which we can yield, but we only want to yield each of them once - so as we see items, 
		//we can remove them from the set, yielding only if that operation was successful. The initial set is formed from the "second" input sequence, 
		//and then we just iterate over the "first" input sequence, removing and yielding appropriately:
		public static IEnumerable<TSource> IntersectBy<TSource, TKey>(this IEnumerable<TSource> first,
			IEnumerable<TSource> second,
			Func<TSource, TKey> keySelector,
			IEqualityComparer<TKey> keyComparer = null)
		{
			HashSet<TKey> potentialElements = new HashSet<TKey>(second.Select(keySelector),
																keyComparer ?? EqualityComparer<TKey>.Default);
			foreach (TSource item in first)
			{
				TKey key = keySelector(item);
				if (potentialElements.Remove(key))
				{
					yield return item;
				}
			}                 
		}
	}
}
