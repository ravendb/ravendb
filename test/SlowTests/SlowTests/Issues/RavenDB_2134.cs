using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.SlowTests.Issues
{
    public class RavenDB_2134 : RavenTestBase
    {
        public RavenDB_2134(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformTheory(RavenTestCategory.Querying, RavenArchitecture.X64)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                QueryStatistics stats;
                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Statistics(out stats)
                        .Where(x => x.Name == "John")
                        .ToList();
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 20000; i++)
                    {
                        await bulkInsert.StoreAsync(new User { Id = i.ToString(), Name = "Name" + i });
                    }
                }

                await Indexes.WaitForIndexingAsync(store,timeout: TimeSpan.FromMinutes(2));
                var queryToDelete = new IndexQuery()
                {
                    Query = $"FROM INDEX '{stats.IndexName}'"
                };

                var operation = await store.Operations.SendAsync(new DeleteByQueryOperation(queryToDelete));
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(2));

                using (var session = store.OpenSession())
                {
                    var count = session
                        .Query<User>(stats.IndexName)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Count();

                    Assert.Equal(0, count);
                }
            }
        }
    }
}