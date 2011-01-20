//-----------------------------------------------------------------------
// <copyright file="EnumerableExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Database.Server.Responders
{
	public static class EnumerableExtensions
	{
		public static IEnumerable<T> EmptyIfNull<T>(this IEnumerable<T> self)
		{
			if (self == null)
				return new T[0];
			return self;
		}
	}
}
