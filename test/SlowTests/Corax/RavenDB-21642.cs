#nullable enable
using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_21642 : RavenTestBase
{
    public RavenDB_21642(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanUseInAndAllInWithOnlyOneElement(Options options)
    {
        using var store = GetDocumentStore(options);
        store.ExecuteIndex(new TestDocumentIndex());

        using (var session = store.OpenSession())
        {
            session.Store(new TestDocument {Tags = null});
            session.Store(new TestDocument {Tags = Array.Empty<string>()});
            session.Store(new TestDocument {Tags = new string[] {"test"}});
            session.Store(new TestDocument {Tags = new string[] {"abc", "test"}});
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var target = new []{"test"};
            int count = session
                .Query<TestDocument, TestDocumentIndex>()
                .Count(d => d.Tags.ContainsAll(target));
            Assert.Equal(2, count);
            
            count = session
                .Query<TestDocument, TestDocumentIndex>()
                .Count(d => d.Tags.In(target));
            Assert.Equal(2, count);
        }
    }

    private class TestDocument
    {
        public string[]? Tags { get; set; }
    }

    private class TestDocumentIndex : AbstractIndexCreationTask<TestDocument>
    {
        public override string IndexName => "indexes/test_document";

        public TestDocumentIndex()
        {
            Map = docs => from doc in docs select new { Tags = doc.Tags == null || doc.Tags.Length == 0 ? null : doc.Tags };
        }
    }

}
