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

public class RavenDB_21338 : RavenTestBase
{
    public RavenDB_21338(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string[] Tags;
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanUpdateEntryWithArrayContainingNull(Options options)
    {
        using var store = GetDocumentStore(options);

        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "idx",
            Maps =
            {
                @"
                from i in docs.Items
                select new 
                {
                    i.Tags
                }"
            },
            Fields = { ["Tags"] = new IndexFieldOptions { Storage = FieldStorage.Yes } }
        }));
 
        using (var s = store.OpenSession())
        {
            s.Store(new Item
            {
                Tags = new string[]{null, null}
            }, "items/1");
            s.SaveChanges();
        }
        Indexes.WaitForIndexing(store);
WaitForUserToContinueTheTest(store);
        using (var s = store.OpenSession())
        {
            Item item = s.Load<Item>("items/1");
            item.Tags = new string[] { null, null, null };
            s.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

    }
}
