using System.IO;
using Lucene.Net.Analysis;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers
{
    public class LowerCaseWhitespaceAnalyzer : LowerCaseKeywordAnalyzer
    {
        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            var res = new LowerCaseWhitespaceTokenizer(reader);
            PreviousTokenStream = res;
            return res;
        }
        public override TokenStream ReusableTokenStream(string fieldName, TextReader reader)
        {
            var previousTokenStream = (LowerCaseWhitespaceTokenizer)PreviousTokenStream;
            if (previousTokenStream == null)
                return TokenStream(fieldName, reader);
            previousTokenStream.Reset(reader);
            return previousTokenStream;
        }

    }
}
