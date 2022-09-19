using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19317 : RavenTestBase
{
    public RavenDB_19317(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CoraxLowercaseTheTermWhenFieldHasFullTextSearchConfiguration(RavenTestBase.Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new Data() {DisplayName = "Jeff Matt"});
            session.SaveChanges();
        }
        new TestIndex().Execute(store);
        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var firstQuery = session.Query<Data, TestIndex>().Count(i => i.DisplayName == "Jeff");
            var secondQuery = session.Query<Data, TestIndex>().Count(i => i.DisplayName == "jeff");
            Assert.Equal(1, firstQuery);
            Assert.Equal(1, secondQuery);
        }
    }

    private class Data
    {
        public string DisplayName { get; set; }
    }

    private class TestIndex : AbstractIndexCreationTask<Data, Data>
    {
        public TestIndex()
        {
            Map = docs => from doc in docs
                select new {DisplayName = doc.DisplayName};
            Index("DisplayName", FieldIndexing.Search);
        }
    }
}
