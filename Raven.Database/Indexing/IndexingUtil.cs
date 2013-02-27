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
			var digitsHash = docId.Where(char.IsDigit).Aggregate(0, (current, ch) => current * 10 + (ch - '0'))
					/ 1024; // will force concentration of more items under the normal case to the same bucket

			var len = docId.Length > 3 ? docId.Length - 2 : docId.Length;// try to achieve a more common prefix

			var nonDigitsHash = docId.Take(len)
				.Aggregate(11, (current, ch) => (char.ToUpperInvariant(ch) & 397) ^ current);

			return Math.Abs(digitsHash) + Math.Abs(nonDigitsHash) % (1024 * 1024);
		}
	}
}