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
	}
}
