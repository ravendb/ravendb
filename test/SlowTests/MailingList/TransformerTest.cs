using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Xunit;

namespace SlowTests.MailingList
{
    public class TransformerTest : RavenTestBase
    {
        private class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Region { get; set; }
        }

        private class SampleCompanyTransformer : AbstractTransformerCreationTask<Company>
        {
            public SampleCompanyTransformer()
            {
                TransformResults = docs => from item in docs
                                           select new
                                           {
                                               Id = item.Id,
                                               SomeProjection = item.Name
                                           };
            }
        }

        [Fact]
        public async Task TestLoadWithTransfomer()
        {
            using (var shard1 = GetDocumentStore())
            using (var shard2 = GetDocumentStore())
            {
                // Ensure transformer on both shards as ShardedDocumentStore does not take care of it
                new SampleCompanyTransformer().Execute(shard1);
                new SampleCompanyTransformer().Execute(shard2);

                // Init strategy
                var shards = new Dictionary<string, IDocumentStore>();
                shards.Add("0", shard1);
                shards.Add("1", shard2);

                var shardStrategy = new ShardStrategy(shards)
                      .ShardingOn<Company>(); // RoundRobin here

                // Init ShardedDocumentStore
                var store = new ShardedDocumentStore(shardStrategy).Initialize();

                // Configure Failover
                store.Conventions.FailoverBehavior = FailoverBehavior.ReadFromAllServers;

                // Create sample data
                var companyShard0 = new Company { Id = "w3qsl3lj4huc", Name = "Company1", Region = "EU" };
                var companyShard1 = new Company { Id = "9gaq9wnzcrzu", Name = "Company2", Region = "US" };

                string[] validIds;
                using (var session = store.OpenAsyncSession())
                {
                    // Note: No proper async / await here. Just for testing
                    await session.StoreAsync(companyShard0);
                    await session.StoreAsync(companyShard1);
                    await session.SaveChangesAsync();

                    // Store generated Ids for later tests
                    validIds = new string[] { companyShard0.Id, companyShard1.Id };
                }

                string myInvalidId = "0/123456";
                var idsToQuery = new string[] { myInvalidId, validIds.First(), validIds.Last() };

                using (var session = store.OpenAsyncSession())
                {
                    // Failing code
                    // Null-Ref here!
                    var results = await session.LoadAsync<SampleCompanyTransformer, dynamic>(ids: idsToQuery.ToList());
                    Assert.NotEmpty(results);
                }

            }
        }
    }
}
