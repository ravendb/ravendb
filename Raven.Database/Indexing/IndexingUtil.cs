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
			int digitsHash = 0, nonDigitsHash = 11;
			int nonDigitsCount = 0, digitsCount = 0;

			for (int i = docId.Length - 1; i >= 0; i--)
			{
				var ch = docId[i];
				if (char.IsDigit(ch))
				{
					digitsHash = (ch - '0') * (int)Math.Pow(10, digitsCount) + digitsHash;
					digitsCount++;

				}
				else
				{
					nonDigitsCount++;
					if (nonDigitsCount == 3) // we are on the third char, so we have more than 2 chars
						nonDigitsHash = 11; // will only hash the len -2 chars, this way we have a more common prefix
					nonDigitsHash = (ch*397) ^ nonDigitsHash;
				}
			}
			digitsHash /= 1024; // will force concentration of more items under the normal case to the same bucket

			return (Math.Abs(digitsHash) + Math.Abs(nonDigitsHash)) % (1024 * 1024);
		}
	}
}