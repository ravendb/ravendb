using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19561 : ReplicationTestBase
    {
        public RavenDB_19561(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Replicate_2_Docs_Which_Is_Bigger_Then_Batch_Size(Options options)
        {
            var co = new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Replication.MaxSizeToSend)] = 1.ToString()
                }
            };
            var node = GetNewServer(co);

            using var store1 = GetDocumentStore(new Options(options) { RunInMemory = false, Server = node, ReplicationFactor = 1 });
            using var store2 = GetDocumentStore(new Options(options) { RunInMemory = false, Server = node, ReplicationFactor = 1 });

            var docs = new List<User>();
            using (var session = store1.OpenAsyncSession())
            {
                var doc = new User
                {
                    Id = $"Users/1-A",
                    Name = $"User1",
                    Info = GenRandomString(2_000_000)
                };
                docs.Add(doc);
                await session.StoreAsync(doc);
                await session.SaveChangesAsync();
            }

            using (var session = store1.OpenAsyncSession())
            {
                var doc = new User
                {
                    Id = $"Users/2-A",
                    Name = $"User2",
                    Info = GenRandomString(2_000_000)
                };
                docs.Add(doc);
                await session.StoreAsync(doc);
                await session.SaveChangesAsync();
            }

            var externalList = await SetupReplicationAsync(store1, store2);

            // wait for replication from store1 to store2/3
            foreach (var doc in docs)
            {
                await WaitAndAssertDocReplicationAsync<User>(store2, doc.Id);
            }
        }

        [Fact]
        public async Task Check_NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded_Configuration_Change()
        {
            var settings = new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Replication.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded)] = "1234",
            };

            var (nodes, leader) = await CreateRaftCluster(3, customSettings: settings);
            foreach (var node in nodes)
            {
                Assert.Equal(node.Configuration.Replication.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded, 1234);
            }
        }

        private async Task WaitAndAssertDocReplicationAsync<T>(DocumentStore store, string id, int timeout = 15_000) where T : class
        {
            var result = await WaitForDocumentToReplicateAsync<User>(store, id, timeout);
            Assert.True(result != null, $"doc \"{id}\" didn't replicated.");
        }

        class User
        {
            public string Id { get; set; }
            public string Name { get; set; }

            public string Info { get; set; }
        }

    }
}
