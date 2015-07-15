using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Version = Lucene.Net.Util.Version;

namespace Raven.Database.Indexing.Analyzers
{
	public class RavenStandardAnalyzer : StandardAnalyzer
	{
		public RavenStandardAnalyzer(Version matchVersion) : base(matchVersion)
		{
			this.matchVersion = matchVersion;
		}

		public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
		{
			StandardTokenizer tokenStream = new StandardTokenizer(matchVersion, reader) {MaxTokenLength = DEFAULT_MAX_TOKEN_LENGTH};			
			var res = new RavenStandardFilter(tokenStream);
			PreviousTokenStream = res;
			return res;
		}

		public override TokenStream ReusableTokenStream(string fieldName, TextReader reader)
		{
			var previousTokenStream = (RavenStandardFilter)PreviousTokenStream;
			if (previousTokenStream == null)
				return TokenStream(fieldName, reader);
			// if the inner tokenazier is successfuly reset
			if (previousTokenStream.Reset(reader))
			{
				return previousTokenStream;
			}
			// we failed so we generate a new token stream
			return TokenStream(fieldName, reader);;
		}
		public const int DEFAULT_MAX_TOKEN_LENGTH = 255;
		private readonly Version matchVersion;  
	}
}
