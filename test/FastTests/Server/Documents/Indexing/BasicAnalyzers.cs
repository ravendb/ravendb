using System.Collections.Generic;
using System.IO;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistance.Lucene;
using Raven.Server.Documents.Indexes.Persistance.Lucene.Analyzers;
using Raven.Tests.Core;

using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class BasicAnalyzers : RavenTestBase
    {
        [Fact]
        public void CheckAnalyzers()
        {
            var operation = new TestOperation();

            var fields = new Dictionary<string, IndexField>();
            fields.Add(Constants.AllFields, new IndexField());
            var analyzer = operation.GetAnalyzer(fields, forQuerying: false);

            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer(string.Empty));
            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer("Unknown_Field"));

            fields.Clear();
            fields.Add(Constants.AllFields, new IndexField { Analyzer = "StandardAnalyzer" });
            analyzer = operation.GetAnalyzer(fields, forQuerying: false);

            Assert.IsType<StandardAnalyzer>(analyzer.GetAnalyzer(string.Empty));
            Assert.IsType<StandardAnalyzer>(analyzer.GetAnalyzer("Unknown_Field"));

            fields.Clear();
            fields.Add("Field1", new IndexField { Analyzer = "StandardAnalyzer" }); // field must be 'NotAnalyzed' or 'Analyzed'
            analyzer = operation.GetAnalyzer(fields, forQuerying: false);

            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer(string.Empty));
            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer("Field1"));

            fields.Clear();
            fields.Add("Field1", new IndexField { Analyzer = "StandardAnalyzer", Indexing = FieldIndexing.NotAnalyzed }); // 'NotAnalyzed' => 'KeywordAnalyzer'
            analyzer = operation.GetAnalyzer(fields, forQuerying: false);

            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer(string.Empty));
            Assert.IsType<KeywordAnalyzer>(analyzer.GetAnalyzer("Field1"));

            fields.Clear();
            fields.Add("Field1", new IndexField { Analyzer = null, Indexing = FieldIndexing.Analyzed }); // 'Analyzed = null' => 'StandardAnalyzer'
            analyzer = operation.GetAnalyzer(fields, forQuerying: false);

            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer(string.Empty));
            Assert.IsType<StandardAnalyzer>(analyzer.GetAnalyzer("Field1"));

            fields.Clear();
            fields.Add("Field1", new IndexField { Analyzer = typeof(NotForQueryingAnalyzer).AssemblyQualifiedName, Indexing = FieldIndexing.Analyzed });
            analyzer = operation.GetAnalyzer(fields, forQuerying: false);

            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer(string.Empty));
            Assert.IsType<NotForQueryingAnalyzer>(analyzer.GetAnalyzer("Field1"));

            fields.Clear();
            fields.Add("Field1", new IndexField { Analyzer = typeof(NotForQueryingAnalyzer).AssemblyQualifiedName, Indexing = FieldIndexing.Analyzed });
            analyzer = operation.GetAnalyzer(fields, forQuerying: true);

            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer(string.Empty));
            Assert.IsType<StandardAnalyzer>(analyzer.GetAnalyzer("Field1"));

            fields.Clear();
            fields.Add("Field1", new IndexField { Analyzer = typeof(NotForQueryingAnalyzer).AssemblyQualifiedName, Indexing = FieldIndexing.Analyzed });
            fields.Add("Field2", new IndexField { Analyzer = "KeywordAnalyzer", Indexing = FieldIndexing.Analyzed });
            analyzer = operation.GetAnalyzer(fields, forQuerying: false);

            Assert.IsType<LowerCaseKeywordAnalyzer>(analyzer.GetAnalyzer(string.Empty));
            Assert.IsType<NotForQueryingAnalyzer>(analyzer.GetAnalyzer("Field1"));
            Assert.IsType<KeywordAnalyzer>(analyzer.GetAnalyzer("Field2"));
        }

        private class TestOperation : IndexOperationBase
        {
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