using System.Linq;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21324 : RavenTestBase
{
    public RavenDB_21324(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string[] Tags;
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanFindNegationToEntryWithEmptyArray(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var s = store.OpenSession())
        {
            s.Store(new Item
            {
                Tags = new string[]{}
            }, "items/1");
            s.SaveChanges();
        }

        using (var s = store.OpenSession())
        {
            Assert.Equal(1, s.Advanced.RawQuery<Item>("from Items where Tags != 'Hamster'").Count());
        }
    }
}
