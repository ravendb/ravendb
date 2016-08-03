using System.ComponentModel.Composition;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;

namespace Raven.Database.Plugins
{
    [InheritedExport]
    public abstract class AbstractAnalyzerGenerator : IRequiresDocumentDatabaseInitialization
    {
        public abstract Analyzer GenerateAnalyzerForIndexing(string indexName, Document document, Analyzer previousAnalyzer);

        public abstract Analyzer GenerateAnalyzerForQuerying(string indexName, string query, Analyzer previousAnalyzer);

        protected internal DocumentDatabase Database { get; set; }

        public void Initialize(DocumentDatabase database)
        {
            Database = database;
            Initialize();
        }

        public virtual void Initialize()
        {

        }

        public virtual void SecondStageInit()
        {

        }
    }
}
