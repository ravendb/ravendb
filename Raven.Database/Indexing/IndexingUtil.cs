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
		public static int StableInvariantIgnoreCaseStringHash(string s)
		{
			return s.Aggregate(11, (current, ch) => (char.ToUpperInvariant(ch).GetHashCode() * 397) ^ current);
		}

		public static int MapBucket(string docId)
		{
			int hash;
			if (char.IsDigit(docId[docId.Length - 1]))// ends with a number, probably users/123, so we will use that
			{
				hash = docId.Where(char.IsDigit).Aggregate(0, (current, ch) => current*10 + (ch - '0'))
					/ 1024; // will force concentration of more items under the normal case to the same bucket
			}
			else
			{
				if (docId.Length > 3) // try to achieve a more common prefix
					docId = docId.Substring(0, docId.Length - 2);
				hash = StableInvariantIgnoreCaseStringHash(docId);
			}
			return Math.Abs(hash) % (1024 * 1024);
		}
	}
}