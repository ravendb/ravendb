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

public class RavenDB_21334 : RavenTestBase
{
    public RavenDB_21334(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanStoreNullDynamicArrayAndGetIndexEntries(Options options)
    {
        using var store = GetDocumentStore(options);

        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "idx",
            Maps =
            {
                @"
                from i in docs.Items
                let owner = LoadDocument(i.Owner, ""Users"")
                let company = LoadDocument(owner.Company, ""Companies"")
                select new 
                {
                    i.Owner,
                    Country = company.Addresses.Select(x => x.Country).ToList()
                }"
            },
            Fields =
            {
                ["Country"] = new IndexFieldOptions
                {
                    Storage = FieldStorage.Yes
                }
            }
        }));

        using (var s = store.OpenSession())
        {
            s.Store(new Item("users/123"));
            s.SaveChanges();
        }
        
        Indexes.WaitForIndexing(store);

        using (var s = store.OpenSession())
        {
            QueryCommand queryCommand = new QueryCommand((InMemoryDocumentSessionOperations)s, new IndexQuery
            {
                Query = "from index idx"
            },metadataOnly: false, indexEntriesOnly: true);
            s.Advanced.RequestExecutor.Execute(queryCommand, s.Advanced.Context, s.Advanced.SessionInfo);
            QueryResult result = queryCommand.Result;
            Assert.Equal(1, result.Results.Length);
            var entry = (BlittableJsonReaderObject)result.Results[0];
            Assert.True(entry.TryGet("Country", out BlittableJsonReaderArray arr));
            Assert.Equal(0, arr.Length);
        }
    }

    private record Item(string Owner);
}
