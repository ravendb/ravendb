using System.Linq;
using FastTests;
using NuGet.ContentModel;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21171 : RavenTestBase
{
    public RavenDB_21171(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void Can_get_accurate_results_when_indexing_missing_property(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var s = store.OpenSession())
        {
            s.Store(new Item(), "1");
            s.Store(new Item(), "2");
            s.Store(new Item(), "3");
            s.SaveChanges();
        }

        using (var s = store.OpenSession())
        {
            s.Advanced.Defer(new PatchCommandData("3", null, new PatchRequest
            {
                Script = "this.Name = 'j' ;"
            }));          
            s.SaveChanges();
        }
        
        new Item_Idx().Execute(store);
        Indexes.WaitForIndexing(store);
        WaitForUserToContinueTheTest(store);
        using (var s = store.OpenSession())
        {
            Assert.Equal(1, s.Advanced.DocumentQuery<Item, Item_Idx>()
                .Statistics(out var indexStats)
                .WhereEquals("Name", "j")
                .Count());

            IndexStats stats = store.Maintenance.Send(new GetIndexStatisticsOperation(indexStats.IndexName));
            // we have just one because we index on Name, and two of those docs don't even _have_ a name property, so it is filtered
            Assert.Equal(1, stats.EntriesCount);
        }

    }

    private record Item();

    private class Item_Idx : AbstractIndexCreationTask
    {
        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition { Maps = { "docs.Items.Select(i => new { i.Name }" } };
        }
    }
}
