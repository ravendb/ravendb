// -----------------------------------------------------------------------
//  <copyright file="IndexingUtil.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Text;

using Raven.Abstractions.Util.Encryptors;

namespace Raven.Database.Indexing
{
	public class IndexingUtil
	{
		public static string FixupIndexName(string indexName, string path)
		{
			if (indexName.EndsWith("=")) //allready encoded
				return indexName;

			indexName = indexName.Trim();

			if (path.Length + indexName.Length <= 230 && Encoding.Unicode.GetByteCount(indexName) < 255)
				return indexName;

			string prefix = null;
			if (indexName.StartsWith("Temp/") || indexName.StartsWith("Auto/"))
				prefix = indexName.Substring(0, 5);

			var bytes = Encryptor.Current.Hash.Compute16(Encoding.UTF8.GetBytes(indexName));
			var result = prefix + Convert
									  .ToBase64String(bytes)
									  .Replace("+", "-"); // replacing + because it will cause IIS errors (double encoding)

			if (path.Length + result.Length > 230)
				throw new InvalidDataException("index name with the given path is too long even after encoding: " + indexName);

			return result;
		}

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