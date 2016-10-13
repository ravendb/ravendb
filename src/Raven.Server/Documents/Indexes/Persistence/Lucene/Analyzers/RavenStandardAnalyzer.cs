using System.IO;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;

using Version = Lucene.Net.Util.Version;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers
{
    public class RavenStandardAnalyzer : StandardAnalyzer
    {
        public RavenStandardAnalyzer(Version matchVersion) : base(matchVersion)
        {
            this.matchVersion = matchVersion;
        }

        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            StandardTokenizer tokenStream = new StandardTokenizer(matchVersion, reader) { MaxTokenLength = DEFAULT_MAX_TOKEN_LENGTH };
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
            return TokenStream(fieldName, reader);
        }

        private readonly Version matchVersion;
    }
}
