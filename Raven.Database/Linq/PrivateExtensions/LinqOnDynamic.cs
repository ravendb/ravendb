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
		private static IEnumerable<dynamic> Select(this object self)
		{
			if (self == null || self is DynamicNullObject)
				yield break;
			if (self is IEnumerable == false || self is string)
				throw new InvalidOperationException("Attempted to enumerate over " + self.GetType().Name);

			foreach (var item in ((IEnumerable) self))
			{
				yield return item;
			}
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
	}
}
