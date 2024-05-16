using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using TimeUnit = Raven.Server.Config.Settings.TimeUnit;

namespace SlowTests.Issues
{
    public class RavenDB_19379 : ClusterTestBase
    {
        public RavenDB_19379(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Node_Stays_Rehab_After_Connect_Again()
        {
            (var nodes, var leader) = await CreateRaftCluster(3, watcherCluster: true, shouldRunInMemory: false);
            var nodeTags = nodes.Select(n => n.ServerStore.NodeTag).ToList();
            using var store = GetDocumentStore(new Options { RunInMemory = false, Server = leader, ReplicationFactor = 3 });

            var nodeToDispose = nodes.First(n => n.ServerStore.IsLeader() == false);
            var nodeToDispose_url = nodeToDispose.WebUrl;
            var nodeToDispose_tag = nodeToDispose.ServerStore.NodeTag;

            var result = await DisposeServerAndWaitForFinishOfDisposalAsync(nodeToDispose);
            await WaitAndAssertRehabs(store, new List<string>() { nodeToDispose_tag }, TimeSpan.FromSeconds(30));

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);
                await session.StoreAsync(new User { Id = "Users/1-A", Name = "FooBar" });
                await session.SaveChangesAsync();
            }

            foreach (var server in Servers.Where(s => s.ServerStore.NodeTag != nodeToDispose_tag))
            {
                server.ServerStore.Configuration.Databases.FrequencyToCheckForIdle = new TimeSetting(1000, TimeUnit.Seconds);

                var database = await Databases.GetDocumentDatabaseInstanceFor(server, store);
                var dbIdEtagDictionary = new Dictionary<string, long>();
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
                using (documentsContext.OpenReadTransaction())
                {
                    foreach (var kvp in DocumentsStorage.GetAllReplicatedEtags(documentsContext))
                        dbIdEtagDictionary[kvp.Key] = kvp.Value;
                }

                Assert.True(server.ServerStore.DatabasesLandlord.UnloadDirectly(database.Name, database.PeriodicBackupRunner.GetNextIdleDatabaseActivity(database.Name)),
                    $"didn't unload on node {server.ServerStore.NodeTag}");
                server.ServerStore.IdleDatabases[database.Name] = dbIdEtagDictionary;

            }

            var cs = new Dictionary<string, string>(DefaultClusterSettings);
            cs[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = nodeToDispose_url;

            using var revivedServer = GetNewServer(new ServerCreationOptions
            {
                DeletePrevious = false,
                RunInMemory = false,
                DataDirectory = result.DataDirectory,
                CustomSettings = cs
            });
            using var revivedStore = new DocumentStore()
            {
                Urls = new[] { nodeToDispose_url }, 
                Database = store.Database, 
                Conventions = new DocumentConventions()
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize();
            var doc = await WaitForDocumentToReplicateAsync<User>(revivedStore, "Users/1-A", TimeSpan.FromSeconds(15));
            Assert.NotNull(doc);

            await WaitAndAssertMembers(store, nodeTags, TimeSpan.FromSeconds(10));
        }
        
        private async Task WaitAndAssertRehabs(DocumentStore store, List<string> expectedRehabs, TimeSpan timeout)
        {
            List<string> actualRehabs=new List<string>();
            Assert.True(await WaitUntilDatabaseHasState(store, timeout: timeout, predicate: record =>
            {
                var rehabs = record?.Topology?.Rehabs;
                actualRehabs.Clear();
                foreach (var v in rehabs)
                {
                    actualRehabs.Add(v);
                }
                return rehabs != null && rehabs.Count == expectedRehabs.Count && ContainsAll(rehabs, expectedRehabs);
            }), $"Rehabs are not as expected.\n Expected Rehabs: {string.Join(' ',expectedRehabs)}\n Actual Rehabs: {string.Join(' ', actualRehabs)}");
        }

        private async Task WaitAndAssertMembers(DocumentStore store, List<string> expectedMembers, TimeSpan timeout)
        {
            List<string> members = new List<string>();
            Assert.True(await WaitUntilDatabaseHasState(store, timeout: timeout, predicate: record =>
            {
                var actualMembers = record?.Topology?.Members;
                members.Clear();
                foreach (var v in actualMembers)
                {
                    members.Add(v);
                }

                return actualMembers != null && ContainsAll(actualMembers, expectedMembers);
            }), $"Members are not as expected. Expected: {String.Join(" ", expectedMembers)}, Actual: {String.Join(" ", members)}");
        }

        private bool ContainsAll<T>(IEnumerable<T> a, IEnumerable<T> b)
        {
            foreach (var v in b)
            {
                if (a.Contains(v) == false)
                    return false;
            }
            return true;
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
