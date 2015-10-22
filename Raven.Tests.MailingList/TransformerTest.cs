using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Tests.Helpers;
using Xunit;

namespace UnitTestProject1
{

    public class TransformerTest : RavenTestBase
    {
        public class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Region { get; set; }
        }

        public class SampleCompanyTransformer : AbstractTransformerCreationTask<Company>
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
            using (var s1 = GetNewServer(port: 8077))
            using (var s2 = GetNewServer(port: 8078))
            using (var shard1 = new DocumentStore { Url = s1.Configuration.ServerUrl}.Initialize())
            using (var shard2 = new DocumentStore { Url = s2.Configuration.ServerUrl }.Initialize())
            {

                // Ensure transformer on both shards as ShardedDocumentStore does not take care of it
                this.EnsureTransformers(shard1);
                this.EnsureTransformers(shard2);

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

        private void EnsureTransformers(IDocumentStore singleShardStore)
        {
            var catalog = new System.ComponentModel.Composition.Hosting.AssemblyCatalog(typeof(TransformerTest).Assembly);
            var container = new System.ComponentModel.Composition.Hosting.CompositionContainer(catalog);
            container.ComposeParts();

            foreach (var task in container.GetExportedValues<AbstractTransformerCreationTask>())
            {
                task.Execute(singleShardStore);
            }
        }
    }
}
