// -----------------------------------------------------------------------
//  <copyright file="PrimaryKeyComparer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Studio.Infrastructure
{
	public class PrimaryKeyComparer<T> : IEqualityComparer<T>
	{
		private readonly Func<T, object> primaryKeyExtractor;

		public PrimaryKeyComparer(Func<T, object> primaryKeyExtractor)
		{
			this.primaryKeyExtractor = primaryKeyExtractor;
		}

		public bool Equals(T x, T y)
		{
			return Equals(primaryKeyExtractor(x), primaryKeyExtractor(y));
		}

		public int GetHashCode(T obj)
		{
			var key = primaryKeyExtractor(obj);
			if (key == null)
				return 0;
			return key.GetHashCode();
		}
	}
}