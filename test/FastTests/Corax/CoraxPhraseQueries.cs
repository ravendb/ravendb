using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class CoraxPhraseQueries : RavenTestBase
{
    public CoraxPhraseQueries(ITestOutputHelper output) : base(output)
    {
    }


    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void StaticPhraseQuery(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new Item("First second third fourth"));
        session.Store(new Item("First third fourth second"));
        session.SaveChanges();
        
        new Index().Execute(store);
        Indexes.WaitForIndexing(store);

        var count = session.Query<Item, Index>().Search(x => x.FtsField, "nonexists \"second third\" nonexsts").Count();
        Assert.Equal(1, count);
    }

    private record Item(string FtsField, string Id = null);

    private class Index : AbstractIndexCreationTask<Item>
    {
        public Index()
        {
            Map = items => items.Select(x => new {FtsField = x.FtsField});

            Index(x => x.FtsField, FieldIndexing.Search);
        }
    }

}
