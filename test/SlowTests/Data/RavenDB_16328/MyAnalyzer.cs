using System.IO;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;

namespace SlowTests.Data.RavenDB_16328
{
    public class MyAnalyzer : StandardAnalyzer
    {
        public MyAnalyzer()
            : base(Lucene.Net.Util.Version.LUCENE_30)
        {
        }

        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            return new StandardFilter(base.TokenStream(fieldName, reader));
        }
    }
}
