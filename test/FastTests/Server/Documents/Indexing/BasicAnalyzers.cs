using System;
using System.Collections.Generic;
using System.IO;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Sparrow.Logging;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class BasicAnalyzers : NoDisposalNeeded
    {
        [Fact]
        public void CheckAnalyzers()
        {
            var operation = new TestOperation("test", null);

            var fields = new Dictionary<string, IndexField>();
            fields.Add(Constants.Documents.Indexing.Fields.AllFields, new IndexField());

            Assert.Throws<InvalidOperationException>(() => operation.GetAnalyzer(fields, forQuerying: false));

            fields.Clear();
            fields.Add(Constants.Documents.Indexing.Fields.AllFields, new IndexField { Analyzer = "StandardAnalyzer" });
            Assert.Throws<InvalidOperationException>(() => operation.GetAnalyzer(fields, forQuerying: false));

            fields.Clear();
            fields.Add("Field1", new IndexField { Analyzer = "StandardAnalyzer" }); // field must be 'NotAnalyzed' or 'Analyzed'
            var analyzer = operation.GetAnalyzer(fields, forQuerying: false);

            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer(string.Empty));
            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer("Field1"));

            fields.Clear();
            fields.Add("Field1", new IndexField { Name = "Field1", Analyzer = "StandardAnalyzer", Indexing = FieldIndexing.Exact }); // 'NotAnalyzed' => 'KeywordAnalyzer'
            analyzer = operation.GetAnalyzer(fields, forQuerying: false);

            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer(string.Empty));
            Assert.IsType<KeywordAnalyzer>(analyzer.GetAnalyzer("Field1"));

            fields.Clear();
            fields.Add("Field1", new IndexField { Name = "Field1", Analyzer = null, Indexing = FieldIndexing.Search }); // 'Analyzed = null' => 'StandardAnalyzer'
            analyzer = operation.GetAnalyzer(fields, forQuerying: false);

            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer(string.Empty));
            Assert.IsType<RavenStandardAnalyzer>(analyzer.GetAnalyzer("Field1"));

            fields.Clear();
            fields.Add("Field1", new IndexField { Name = "Field1", Analyzer = typeof(NotForQueryingAnalyzer).AssemblyQualifiedName, Indexing = FieldIndexing.Search });
            analyzer = operation.GetAnalyzer(fields, forQuerying: false);

            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer(string.Empty));
            Assert.IsType<NotForQueryingAnalyzer>(analyzer.GetAnalyzer("Field1"));

            fields.Clear();
            fields.Add("Field1", new IndexField { Name = "Field1", Analyzer = typeof(NotForQueryingAnalyzer).AssemblyQualifiedName, Indexing = FieldIndexing.Search });
            analyzer = operation.GetAnalyzer(fields, forQuerying: true);

            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer(string.Empty));
            Assert.IsType<RavenStandardAnalyzer>(analyzer.GetAnalyzer("Field1"));

            fields.Clear();
            fields.Add("Field1", new IndexField { Name = "Field1", Analyzer = typeof(NotForQueryingAnalyzer).AssemblyQualifiedName, Indexing = FieldIndexing.Search });
            fields.Add("Field2", new IndexField { Name = "Field2", Analyzer = "KeywordAnalyzer", Indexing = FieldIndexing.Search });
            analyzer = operation.GetAnalyzer(fields, forQuerying: false);

            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer(string.Empty));
            Assert.IsType<NotForQueryingAnalyzer>(analyzer.GetAnalyzer("Field1"));
            Assert.IsType<KeywordAnalyzer>(analyzer.GetAnalyzer("Field2"));
        }

        private class TestOperation : IndexOperationBase
        {
            public TestOperation(string indexName, Logger logger) : base(indexName, logger)
            {
            }

            public RavenPerFieldAnalyzerWrapper GetAnalyzer(Dictionary<string, IndexField> fields, bool forQuerying)
            {
                return CreateAnalyzer(() => new LowerCaseKeywordAnalyzer(), fields, forQuerying);
            }

            public override void Dispose()
            {
            }
        }

        [NotForQuerying]
        private class NotForQueryingAnalyzer : Analyzer
        {
            public override TokenStream TokenStream(string fieldName, TextReader reader)
            {
                throw new System.NotImplementedException();
            }
        }
    }
}
