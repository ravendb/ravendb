using System.IO;
using Lucene.Net.Analysis;

namespace Raven.Database.Indexing
{
	public class LowerCaseWhitespaceAnalyzer : LowerCaseKeywordAnalyzer
	{
		public override TokenStream TokenStream(string fieldName, TextReader reader)
		{
			return new LowerCaseWhitespaceTokenizer(reader);
		}
	}
}