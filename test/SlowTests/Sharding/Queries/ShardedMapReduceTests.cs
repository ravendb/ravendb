using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Smuggler;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Queries
{
    public class ShardedMapReduceTests : RavenTestBase
    {
        public ShardedMapReduceTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Indexes | RavenTestCategory.Smuggler)]
        public async Task Can_Export_And_Import_Auto_Map_Reduce_Index()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore(new Options
                   {
                       ModifyDatabaseName = _ => store1.Database + "_restored"
                   }))
            {
                string indexName = null;

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jane", LastName = "Doe", Count = 10 }, "users/1");
                    await session.StoreAsync(new User { Name = "Jane", LastName = "Doe", Count = 10 }, "users/2");
                    await session.StoreAsync(new User { Name = "Grisha", LastName = "Kotler", Count = 21 }, "users/3$3");
                    await session.StoreAsync(new User { Name = "Jane", LastName = "Doe", Count = 10 }, "users/4$3");
                    await session.SaveChangesAsync();

                    var result = await session.Query<User>()
                        .Statistics(out var stats)
                        .GroupBy(x => new { x.Name, x.LastName }).Select(x => new
                        {
                            Name = x.Key.Name,
                            LastName = x.Key.LastName,
                            Sum = x.Sum(u => u.Count)
                        })
                        .Take(1)
                        .ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("Grisha", result[0].Name);
                    Assert.Equal("Kotler", result[0].LastName);
                    Assert.Equal(21, result[0].Sum);

                    indexName = stats.IndexName;
                }

                string tempFileName = GetTempFileName();

                var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), tempFileName);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), tempFileName);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                await Indexes.WaitForIndexingAsync(store2);

                using (var session = store2.OpenAsyncSession())
                {
                    var result = await session.Query<User>(indexName)
                        .Take(1)
                        .As<AutoMapReduceResult>()
                        .ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("Grisha", result[0].Name);
                    Assert.Equal("Kotler", result[0].LastName);
                    Assert.Equal(21, result[0].Count);
                }
            }
        }

        private class AutoMapReduceResult
        {
            public string Name { get; set; }
            public string LastName { get; set; }
            public int Count { get; set; }
        }
    }
}
