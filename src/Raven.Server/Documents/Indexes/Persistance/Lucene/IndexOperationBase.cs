using System;

namespace Raven.Server.Documents.Indexes.Persistance.Lucene
{
    public abstract class IndexOperationBase : IDisposable
    {
        protected LowerCaseKeywordAnalyzer CreateAnalyzer(LowerCaseKeywordAnalyzer defaultAnalyzer)
        {
            // TODO [ppekrol] support for RavenPerFieldAnalyzerWrapper

            return defaultAnalyzer;
        }

        public abstract void Dispose();
    }
}