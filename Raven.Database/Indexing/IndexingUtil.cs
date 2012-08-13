// -----------------------------------------------------------------------
//  <copyright file="IndexingUtil.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

namespace Raven.Database.Indexing
{
	public class IndexingUtil
	{
		public static int AbsStableInvariantIgnoreCaseStringHash(string s)
		{
			return Math.Abs(s.Aggregate(11, (current, ch) => (char.ToUpperInvariant(ch).GetHashCode() * 397) ^ current));
		} 
	}
}