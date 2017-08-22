using System;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8161 : RavenTestBase
    {
        [Fact]
        public void Can_delete_all_entries_from_compressed_tree_in_map_reduce_index()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(path: path))
            {
                store.Admin.Send(new CreateSampleDataOperation());

                for (int i = 0; i < 3; i++)
                {
                    store.Operations.Send(new PatchByQueryOperation(new IndexQuery
                    {
                        Query = "FROM Orders"
                    }, new PatchRequest
                    {
                        Script = @"put(""orders/"", this);"
                    })).WaitForCompletion(TimeSpan.FromSeconds(30));
                }

                WaitForIndexing(store);

                var operation = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery() { Query = "FROM orders" }));
                operation.WaitForCompletion(TimeSpan.FromSeconds(60));

                WaitForIndexing(store);

                var indexStats = store.Admin.Send(new GetIndexesStatisticsOperation());

                foreach (var stats in indexStats)
                {
                    Assert.Equal(0, stats.ErrorsCount);
                }
            }
        }
    }
}
