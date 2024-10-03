﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Xunit;
using Xunit.Abstractions;
using Index = Raven.Server.Documents.Indexes.Index;

namespace FastTests.Server.Documents.Indexing
{
    public class BasicAnalyzers : RavenTestBase
    {
        public BasicAnalyzers(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CheckAnalyzers()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = RavenConfiguration.CreateForTesting("test", Raven.Server.ServerWide.ResourceType.Server);
                configuration.Initialize();

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                var index = new TestIndex(database, configuration.Indexing);

                var operation = new TestOperation(index, null);

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
        }

        [Fact]
        public async Task OverrideAnalyzers()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = RavenConfiguration.CreateForTesting("test", Raven.Server.ServerWide.ResourceType.Server);
                configuration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.DefaultAnalyzer), "WhitespaceAnalyzer");
                configuration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.DefaultExactAnalyzer), "StandardAnalyzer");
                configuration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.DefaultSearchAnalyzer), "KeywordAnalyzer");

                configuration.Initialize();

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                var index = new TestIndex(database, configuration.Indexing);

                var operation = new TestOperation(index, null);

                var fields = new Dictionary<string, IndexField>();
                fields.Add(Constants.Documents.Indexing.Fields.AllFields, new IndexField());

                Assert.Throws<InvalidOperationException>(() => operation.GetAnalyzer(fields, forQuerying: false));

                fields.Clear();
                fields.Add(Constants.Documents.Indexing.Fields.AllFields, new IndexField { Analyzer = "StandardAnalyzer" });
                Assert.Throws<InvalidOperationException>(() => operation.GetAnalyzer(fields, forQuerying: false));

                fields.Clear();
                fields.Add("Field1", new IndexField { Analyzer = "StandardAnalyzer" }); // field must be 'NotAnalyzed' or 'Analyzed'
                var analyzer = operation.GetAnalyzer(fields, forQuerying: false);

                Assert.IsType<WhitespaceAnalyzer>(analyzer.GetAnalyzer(string.Empty));
                Assert.IsType<WhitespaceAnalyzer>(analyzer.GetAnalyzer("Field1"));

                fields.Clear();
                fields.Add("Field1", new IndexField { Name = "Field1", Analyzer = "StandardAnalyzer", Indexing = FieldIndexing.Exact }); // 'NotAnalyzed' => 'StandardAnalyzer'
                analyzer = operation.GetAnalyzer(fields, forQuerying: false);

                Assert.IsType<WhitespaceAnalyzer>(analyzer.GetAnalyzer(string.Empty));
                Assert.IsType<StandardAnalyzer>(analyzer.GetAnalyzer("Field1"));

                fields.Clear();
                fields.Add("Field1", new IndexField { Name = "Field1", Analyzer = null, Indexing = FieldIndexing.Search }); // 'Analyzed = null' => 'KeywordAnalyzer'
                analyzer = operation.GetAnalyzer(fields, forQuerying: false);

                Assert.IsType<WhitespaceAnalyzer>(analyzer.GetAnalyzer(string.Empty));
                Assert.IsType<KeywordAnalyzer>(analyzer.GetAnalyzer("Field1"));

                fields.Clear();
                fields.Add("Field1", new IndexField { Name = "Field1", Analyzer = typeof(NotForQueryingAnalyzer).AssemblyQualifiedName, Indexing = FieldIndexing.Search });
                analyzer = operation.GetAnalyzer(fields, forQuerying: false);

                Assert.IsType<WhitespaceAnalyzer>(analyzer.GetAnalyzer(string.Empty));
                Assert.IsType<NotForQueryingAnalyzer>(analyzer.GetAnalyzer("Field1"));

                fields.Clear();
                fields.Add("Field1", new IndexField { Name = "Field1", Analyzer = typeof(NotForQueryingAnalyzer).AssemblyQualifiedName, Indexing = FieldIndexing.Search });
                analyzer = operation.GetAnalyzer(fields, forQuerying: true);

                Assert.IsType<WhitespaceAnalyzer>(analyzer.GetAnalyzer(string.Empty));
                Assert.IsType<KeywordAnalyzer>(analyzer.GetAnalyzer("Field1"));

                fields.Clear();
                fields.Add("Field1", new IndexField { Name = "Field1", Analyzer = typeof(NotForQueryingAnalyzer).AssemblyQualifiedName, Indexing = FieldIndexing.Search });
                fields.Add("Field2", new IndexField { Name = "Field2", Analyzer = "KeywordAnalyzer", Indexing = FieldIndexing.Search });
                analyzer = operation.GetAnalyzer(fields, forQuerying: false);

                Assert.IsType<WhitespaceAnalyzer>(analyzer.GetAnalyzer(string.Empty));
                Assert.IsType<NotForQueryingAnalyzer>(analyzer.GetAnalyzer("Field1"));
                Assert.IsType<KeywordAnalyzer>(analyzer.GetAnalyzer("Field2"));
            }
        }

        private class TestOperation : IndexOperationBase
        {
            public TestOperation(Index index, RavenLogger logger) : base(index, logger)
            {
            }

            public LuceneRavenPerFieldAnalyzerWrapper GetAnalyzer(Dictionary<string, IndexField> fields, bool forQuerying)
            {
                return LuceneIndexingHelpers.CreateLuceneAnalyzer(_index, new TestIndexDefinitions
                {
                    IndexFields = fields
                }, forQuerying);
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

    internal class TestIndex : Index
    {
        public TestIndex(DocumentDatabase database, IndexingConfiguration configuration) : base(IndexType.None, IndexSourceType.None, new TestIndexDefinitions())
        {
            DocumentDatabase = database;
            Configuration = configuration;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            throw new NotImplementedException();
        }

        public override IIndexedItemEnumerator GetMapEnumerator(IEnumerable<IndexItem> documents, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats, IndexType type)
        {
            throw new NotImplementedException();
        }

        public override void HandleDelete(Tombstone tombstone, string collection, Lazy<IndexWriteOperationBase> writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotImplementedException();
        }

        public override int HandleMap(IndexItem indexItem, IEnumerable mapResults, Lazy<IndexWriteOperationBase> writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotImplementedException();
        }

        public override (ICollection<string> Static, ICollection<string> Dynamic) GetEntriesFields()
        {
            throw new NotImplementedException();
        }

        public override IQueryResultRetriever GetQueryResultRetriever(IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext documentsContext, SearchEngineType searchEngineType, FieldsToFetch fieldsToFetch,
            IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, IncludeRevisionsCommand includeRevisionsCommand)
        {
            throw new NotImplementedException();
        }

        public override void SaveLastState()
        {
            throw new NotImplementedException();
        }
    }

    internal class TestIndexDefinitions : IndexDefinitionBaseServerSide
    {
        public override long Version => IndexVersion.CurrentVersion;

        public TestIndexDefinitions()
        {
            Collections = new HashSet<string> { Constants.Documents.Collections.AllDocumentsCollection };
        }

        public override void Persist(TransactionOperationContext context, StorageEnvironmentOptions options)
        {
            throw new NotImplementedException();
        }

        protected override void PersistMapFields(JsonOperationContext context, AbstractBlittableJsonTextWriter writer)
        {
            throw new NotImplementedException();
        }

        protected override void PersistFields(JsonOperationContext context, AbstractBlittableJsonTextWriter writer)
        {
            throw new NotImplementedException();
        }

        protected internal override IndexDefinition GetOrCreateIndexDefinitionInternal()
        {
            throw new NotImplementedException();
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinitionBaseServerSide indexDefinition)
        {
            throw new NotImplementedException();
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinition indexDefinition)
        {
            throw new NotImplementedException();
        }

        internal override void Reset()
        {
            throw new NotImplementedException();
        }
    }
}
