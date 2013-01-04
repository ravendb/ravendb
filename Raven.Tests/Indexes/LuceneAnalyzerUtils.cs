//-----------------------------------------------------------------------
// <copyright file="LuceneAnalyzerUtils.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;

namespace Raven.Tests.Indexes
{
	public class LuceneAnalyzerUtils
	{
		[CLSCompliant(false)]
		public static IEnumerable<string> TokensFromAnalysis(Analyzer analyzer, String text)
		{
			using (TokenStream stream = analyzer.TokenStream("contents", new StringReader(text)))
			{
				var result = new List<string>();
				var tokenAttr = (TermAttribute) stream.GetAttribute<ITermAttribute>();

				while (stream.IncrementToken())
				{
					result.Add(tokenAttr.Term);
				}

				stream.End();

				return result;
			}
		}
	}
}
