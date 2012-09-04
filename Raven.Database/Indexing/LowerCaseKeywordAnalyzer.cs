//-----------------------------------------------------------------------
// <copyright file="LowerCaseAnalyzer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Lucene.Net.Analysis;
using Lucene.Net.Util;

namespace Raven.Database.Indexing
{
	
	public class LowerCaseKeywordAnalyzer : Analyzer
	{
		public override TokenStream ReusableTokenStream(string fieldName, TextReader reader)
		{
			var previousTokenStream = (LowerCaseKeywordTokenizer)PreviousTokenStream;
			if (previousTokenStream == null)
				return TokenStream(fieldName, reader);
			previousTokenStream.Reset(reader);
			return previousTokenStream;
		}

		public override TokenStream TokenStream(string fieldName, TextReader reader)
		{
			return new LowerCaseKeywordTokenizer(reader);
		}

		public class LowerCaseKeywordTokenizer : CharTokenizer
		{
			public LowerCaseKeywordTokenizer(TextReader input) : base(input)
			{
			}

			public LowerCaseKeywordTokenizer(AttributeSource source, TextReader input) : base(source, input)
			{
			}

			public LowerCaseKeywordTokenizer(AttributeFactory factory, TextReader input) : base(factory, input)
			{
			}

			protected override bool IsTokenChar(char c)
			{
				return true;// everything is a valid character
			}
			protected override char Normalize(char c)
			{
				return char.ToLowerInvariant(c);
			}
		}
	}
}
