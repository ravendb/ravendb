using System.IO;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;

namespace SlowTests.Data.RavenDB_14939
{
    public class MyAnalyzer : StandardAnalyzer
    {
        public MyAnalyzer()
            : base(Lucene.Net.Util.Version.LUCENE_30)
        {
        }

        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            return new ASCIIFoldingFilter(base.TokenStream(fieldName, reader));
        }
    }
}
