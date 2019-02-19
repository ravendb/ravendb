using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client
{
    public class IndexesDeleteByQueryTests : RavenTestBase
    {
        [Fact]
        public void Delete_By_Index()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" });
                    session.Store(new User { Name = "Arek" });
                    session.SaveChanges();
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

                var operation = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = $"FROM INDEX '{indexName}'" }, new QueryOperationOptions { AllowStale = false }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(60));

                var databaseStatistics = store.Maintenance.Send(new GetStatisticsOperation());

                Assert.Equal(1, databaseStatistics.CountOfDocuments);
            }
        }

        [Fact]
        public async Task Delete_By_Index_Async()
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
                    var people = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out QueryStatistics stats)
                        .Where(x => x.Name == "Arek")
                        .ToList();

                    indexName = stats.IndexName;
                }

                var operation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery { Query = $"FROM INDEX '{indexName}'" }, new QueryOperationOptions { AllowStale = false }));

                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var databaseStatistics = store.Maintenance.Send(new GetStatisticsOperation());

                Assert.Equal(0, databaseStatistics.CountOfDocuments);
            }
        }
    }
}
