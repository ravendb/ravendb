//-----------------------------------------------------------------------
// <copyright file="CollationKeyFilter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;

namespace Raven.Database.Indexing.Collation
{
	public class CollationKeyFilter : TokenFilter
	{
		private readonly TermAttribute termAtt;
		private readonly CultureInfo cultureInfo;

		public CollationKeyFilter(TokenStream input, CultureInfo cultureInfo) : base(input)
		{
			this.cultureInfo = cultureInfo;
			termAtt = (TermAttribute)base.AddAttribute <ITermAttribute>();
		}

		public override bool IncrementToken()
		{
			if (input.IncrementToken())
			{
				char[] termBuffer = termAtt.TermBuffer();
				var termText = new String(termBuffer, 0, termAtt.TermLength());
				byte[] collationKey = cultureInfo.CompareInfo.GetSortKey(termText).KeyData;
				int encodedLength = IndexableBinaryStringTools_UsingArrays.GetEncodedLength(collationKey);
				if (encodedLength > termBuffer.Length)
				{
					termAtt.ResizeTermBuffer(encodedLength);
				}
				termAtt.SetTermLength(encodedLength);
				IndexableBinaryStringTools_UsingArrays.Encode(collationKey, termBuffer);
				return true;
			}
			return false;
		}
	}
}