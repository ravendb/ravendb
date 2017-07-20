using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Indexing
{
    public class IndexesFromClient : RavenTestBase
    {
        [Fact]
        public async Task DeleteByIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.StoreAsync(new User { Name = "Arek" });

                    await session.SaveChangesAsync();
                }

                string indexName;
                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var people = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats)
                        .Where(x => x.Name == "Arek")
                        .ToList();

                    indexName = stats.IndexName;
                }

                var operation = await store
                    .Operations
                    .SendAsync(new DeleteByIndexOperation(indexName, new IndexQuery(), new QueryOperationOptions { AllowStale = false }));

                var deleteResult = await operation
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false) as BulkOperationResult;

                Assert.Equal(2, deleteResult.Total);

                var statistics = await store
                    .Admin
                    .SendAsync(new GetStatisticsOperation());

                Assert.Equal(1, statistics.CountOfDocuments);

                using (var session = store.OpenAsyncSession())
                {
                    var hilo = await session.LoadAsync<dynamic>("Raven/Hilo/users");
                    Assert.NotNull(hilo);

                    var stats = await store.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                }

                await store.Admin.SendAsync(new StopIndexingOperation());

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.StoreAsync(new User { Name = "Arek" });

                    await session.SaveChangesAsync();
                }

                operation = await store
                    .Operations
                    .SendAsync(new DeleteByIndexOperation(indexName, new IndexQuery(), new QueryOperationOptions { AllowStale = false }));

                var e = Assert.Throws<RavenException>(() =>
                {
                    operation.WaitForCompletion(TimeSpan.FromSeconds(15));
                });

                Assert.Contains("Query is stale", e.Message);
            }
        }

        [Fact]
        public async Task UpdateByIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.StoreAsync(new User { Name = "Arek" }, "users/2");

                    await session.SaveChangesAsync();
                }

                string indexName;
                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var people = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats)
                        .Where(x => x.Name == "Arek")
                        .ToList();

                    indexName = stats.IndexName;
                }

                var operation = await store
                    .Operations
                    .SendAsync(new PatchByIndexOperation(indexName, new IndexQuery(), new PatchRequest { Script = "this.LastName = 'Test';" }, new QueryOperationOptions { AllowStale = false }));

                await operation
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    var user2 = session.Load<User>("users/2");

                    Assert.Equal("Test", user1.LastName);
                    Assert.Equal("Test", user2.LastName);
                }
            }
        }
    }
}