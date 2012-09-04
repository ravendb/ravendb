//-----------------------------------------------------------------------
// <copyright file="CollationAnalyzer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;
using System.IO;
using Lucene.Net.Analysis;

namespace Raven.Database.Indexing.Collation
{
	public class CollationAnalyzer : Analyzer
	{
		private CultureInfo cultureInfo;

		public CollationAnalyzer(CultureInfo cultureInfo)
		{
			Init(cultureInfo);
		}

		protected void Init(CultureInfo ci)
		{
			cultureInfo = ci;
		}

		protected CollationAnalyzer()
		{
			
		}

		public override TokenStream TokenStream(string fieldName, TextReader reader)
		{
			TokenStream result = new KeywordTokenizer(reader);
			return new CollationKeyFilter(result, cultureInfo);
		}

		private class SavedStreams
		{
			public Tokenizer source;
			public TokenStream result;
		}

		public override TokenStream ReusableTokenStream(string fieldName, TextReader reader)
		{
			var streams = (SavedStreams)PreviousTokenStream;
			if (streams == null)
			{
				streams = new SavedStreams();
				streams.source = new KeywordTokenizer(reader);
				streams.result = new CollationKeyFilter(streams.source, cultureInfo);
				PreviousTokenStream = streams;
			}
			else
			{
				streams.source.Reset(reader);
			}
			return streams.result;
		}
	}
}