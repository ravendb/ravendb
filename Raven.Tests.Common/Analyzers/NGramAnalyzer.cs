// -----------------------------------------------------------------------
//  <copyright file="NGramAnalyzer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Util;

using Raven.Database.Indexing;

namespace Raven.Tests.Common.Analyzers
{
	[NotForQuerying]
	public class NGramAnalyzer : Analyzer
	{
		public override TokenStream TokenStream(string fieldName, TextReader reader)
		{
			var tokenizer = new StandardTokenizer(Version.LUCENE_29, reader);
			tokenizer.MaxTokenLength = 255;
			TokenStream filter = new StandardFilter(tokenizer);
			filter = new LowerCaseFilter(filter);
			filter = new StopFilter(false, filter, StandardAnalyzer.STOP_WORDS_SET);
			return new NGramTokenFilter(filter, 2, 6);
		}
	}
}