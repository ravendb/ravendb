using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26607 : RavenTestBase
{
    public RavenDB_26607(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void Querying_A_Non_Indexed_Field_Should_Throw_In_Corax(Options options)
    {
        using var store = GetDocumentStore(options);
        
        store.ExecuteIndex(new TestIndex());

        using (var session = store.OpenSession())
        {
            session.Store(new Mitarbeiter { Id = "1", Name = "MA 1" });
            session.Store(new Mitarbeiter { Id = "2", Name = "MA 2" });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var query = session.Query<TestIndex.Result, TestIndex>()
                .Customize(c => c.WaitForNonStaleResults())
                .Where(r => r.Prop != null);

            if (options.SearchEngineMode == RavenSearchEngineMode.Corax)
            {
                // 'Prop' is configured with FieldIndexing.No, so Corax has no terms to evaluate the filter against.
                // Previously the query silently returned every document (AndNot(AllEntries, <empty>)); now it fails loudly.
                var error = Assert.Throws<InvalidQueryException>(() => query.ToList());
                Assert.Contains("not indexed", error.Message);
            }
            else
            {
                // Lucene always indexes a null marker for stored fields, so the filter is evaluated and correctly matches nothing.
                Assert.Empty(query.ToList());
            }
        }
    }

    private class TestIndex : AbstractIndexCreationTask<Mitarbeiter, TestIndex.Result>
    {

        public class Result
        {
            public string Name { get; set; }
            public string Prop { get; set; }
        }

        public TestIndex()
        {
            Map = mitarbeitende => from mitarbeiter in mitarbeitende
                                   select new Result
                                   {
                                       Name = mitarbeiter.Name,
                                       Prop = null
                                   };

            StoreAllFields(FieldStorage.Yes);
            Index(r => r.Prop, FieldIndexing.No);

        }
    }

    private class Mitarbeiter
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
