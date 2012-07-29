//-----------------------------------------------------------------------
// <copyright file="LinqOnDynamic.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Linq;

namespace Raven.Database.Linq.PrivateExtensions
{
	/// <summary>
	/// 	These methods allow the indexes to use Linq query syntax using dynamic
	/// </summary>
	public static class LinqOnDynamic
	{
		public class WrapperGrouping : DynamicList, IGrouping<object, object>
		{
			private readonly IGrouping<dynamic, dynamic> inner;

			public WrapperGrouping(IGrouping<dynamic, dynamic> inner)
				: base(inner)
			{
				this.inner = inner;
			}

			public dynamic Key
			{
				get { return inner.Key; }
			}

			IEnumerator IEnumerable.GetEnumerator()
			{
				return GetEnumerator();
			}
		}

		public static IEnumerable<IGrouping<dynamic, dynamic>> GroupBy(this IEnumerable<dynamic> source, Func<dynamic, dynamic> keySelector)
		{
			return Enumerable.GroupBy(source, keySelector).Select(inner => new WrapperGrouping(inner));
		}

		private static IEnumerable<dynamic> Select(this object self)
		{
			if (self == null || self is DynamicNullObject)
				yield break;
			if (self is IEnumerable == false || self is string)
				throw new InvalidOperationException("Attempted to enumerate over " + self.GetType().Name);

			foreach (var item in ((IEnumerable)self))
			{
				yield return item;
			}
		}

		public static IEnumerable<dynamic> DefaultIfEmpty(this IEnumerable<dynamic> self)
		{
			return self.DefaultIfEmpty<dynamic>(new DynamicNullObject());
		}

		public static IEnumerable<dynamic> SelectMany(this object source,
													  Func<dynamic, int, IEnumerable<dynamic>> collectionSelector,
													  Func<dynamic, dynamic, dynamic> resultSelector)
		{
			return Enumerable.SelectMany(Select(source), collectionSelector, resultSelector);
		}

		public static IEnumerable<dynamic> SelectMany(this object source,
													  Func<dynamic, IEnumerable<dynamic>> collectionSelector,
													  Func<dynamic, dynamic, dynamic> resultSelector)
		{
			return Enumerable.SelectMany(Select(source), collectionSelector, resultSelector);
		}

		public static IEnumerable<dynamic> SelectMany(this object source,
													  Func<dynamic, IEnumerable<dynamic>> selector)
		{
			return Select(source).SelectMany<object, object>(selector);
		}

		public static dynamic FirstOrDefault(this IGrouping<dynamic, dynamic> source, Func<dynamic, bool> predicate)
		{
			return Enumerable.FirstOrDefault(source, predicate) ?? new DynamicNullObject();
		}

		public static dynamic SingleOrDefault(this IGrouping<dynamic, dynamic> source, Func<dynamic, bool> predicate)
		{
			return Enumerable.SingleOrDefault(source, predicate) ?? new DynamicNullObject();
		}
	}
}
