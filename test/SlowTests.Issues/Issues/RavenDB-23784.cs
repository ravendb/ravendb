using System;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using System.Linq;
using Corax.Pipeline;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers.Collation.Cultures;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_23784(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Indexes)]
    [InlineData(true)]
    [InlineData(false)]
    public void AnalyzerThatDoesNotOverrideTheTokenStreamWillReturnCorrectTerms(bool forWriter)
    {
        LuceneAnalyzerAdapter analyzer = forWriter 
            ? LuceneAnalyzerAdapterForWriter.Create(new DeCollationAnalyzer())
            : LuceneAnalyzerAdapterForQuerying.Create(new DeCollationAnalyzer());
        Span<byte> dst = new byte[1024];
        Span<Token> tks = new Token[1024];

        for (int i = 0; i < 8; ++i)
        {
            var dstCpy = dst;
            var tksCpy = tks;
            analyzer.Execute("Ö"u8, ref dstCpy, ref tksCpy);
            Assert.True(1 == tksCpy.Length, $"Expected 1 token, got: {tksCpy.Length} at iteration no. {i}.");
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void CanGenerateProperTermsViaAnalyzerThatDoesNotOverrideTokenStream(Options options)
    {
        using var store = GetDocumentStore(options);
        store.ExecuteIndex(new TestIndex());
        
        using (var session = store.OpenSession())
        {
            session.Store(new TestDocument { Name = "Z" });
            session.Store(new TestDocument { Name = "Ö" });
            session.Store(new TestDocument { Name = "Z" });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
            
        using (var session = store.OpenSession())
        {
            var querySorted = session.Query<TestDocument, TestIndex>()
                .OrderBy(x => x.Name)
                .ProjectInto<TestIndex.Result>()
                .ToList();
            
            var queryUnsorted = session.Query<TestDocument, TestIndex>()
                .ProjectInto<TestIndex.Result>()
                .ToList();
                
            Assert.Equal(queryUnsorted.Count, querySorted.Count);
            Assert.StartsWith("Ö", querySorted[0].Name);
        }
    }

    private class TestDocument
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class TestIndex : AbstractIndexCreationTask<TestDocument, TestIndex.Result>
    {
        public class Result
        {
            public string Name { get; set; }
        }

        public TestIndex()
        {
            Map = collection => from entity in collection
                select new
                {
                    Name = entity.Name
                };
            
            Analyze(x => x.Name, 
                "Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers.Collation.Cultures.DeCollationAnalyzer, Raven.Server");
            StoreAllFields(FieldStorage.Yes);
        }
    }
}
