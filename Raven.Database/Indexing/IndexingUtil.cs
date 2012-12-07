// -----------------------------------------------------------------------
//  <copyright file="IndexingUtil.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Raven.Database.Indexing
{
	public class IndexingUtil
	{
		public static int AbsStableInvariantIgnoreCaseStringHash(string s)
		{
			return Math.Abs(s.Aggregate(11, (current, ch) => (char.ToUpperInvariant(ch).GetHashCode() * 397) ^ current));
		}

		public static int MapBucket(string docId)
		{
			var hash = AbsStableInvariantIgnoreCaseStringHash(docId);
			return hash % (1024 * 1024);
		}
	}
}