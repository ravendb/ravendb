using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Esprima.Ast;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace SlowTests.Issues
{
    public class RavenDB_17347 : ClusterTestBase
    {
        public RavenDB_17347(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Database_Group_Node_State_Doesnt_Take_Into_Account_Replication_Of_ACompareExchanges()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            (var nodes, var leader) = await CreateRaftCluster(3, shouldRunInMemory: false, watcherCluster: true);

            leader.ServerStore.Engine.ForTestingPurposes = new RachisConsensus.TestingStuff()
            {
                Mre = null
            };

            using var store = GetDocumentStore(new Options { RunInMemory = false, Server = leader, ReplicationFactor = 2 });
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var topologyNodeTag = record.Topology.AllNodes.ToList();

            var node2inDb = nodes.First(n => n.ServerStore.IsLeader() == false && topologyNodeTag.Contains(n.ServerStore.NodeTag) );
            var node1inDb = nodes.First(n => topologyNodeTag.Contains(n.ServerStore.NodeTag) && n.ServerStore.NodeTag!= node2inDb.ServerStore.NodeTag);

            DisconnectNode(new List<RavenServer>(){ leader }, node2inDb.ServerStore.NodeTag);
            var result = await DisposeServerAndWaitForFinishOfDisposalAsync(node2inDb); // for not saving on this node

            var objForCmpexch = new TestObj()
            {
                Id = "testObjs/0",
                Prop = "testObjs0_prop"
            };

            var objForDocument = new TestObj()
            {
                Id = "testObjs/1",
                Prop = "testObjs1_prop"
            };


            // wait for node2inDb to be rehab
            await WaitAndAssertRehabs(store, new List<string>() { node2inDb.ServerStore.NodeTag });

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(objForDocument);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(objForCmpexch.Id, objForCmpexch);
                await session.SaveChangesAsync();
            }
            
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(objForCmpexch.Id);
                Assert.Equal(objForCmpexch.Prop, entity.Value.Prop);
            }
            
            // restart
            GetNewServer(new ServerCreationOptions
            {
                DeletePrevious = false,
                RunInMemory = false,
                DataDirectory = result.DataDirectory,
                CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url }
            });

            await WaitAndAssertRehabs(store, new List<string>() { node2inDb.ServerStore.NodeTag });

            ReconnectNode(new List<RavenServer>() { leader }, node2inDb.ServerStore.NodeTag);

            // wait for all to be members
            await WaitAndAssertMembers(store, new List<string>() { node1inDb.ServerStore.NodeTag, node2inDb.ServerStore.NodeTag });

            var entity1 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<TestObj>(objForCmpexch.Id, node1inDb.ServerStore.NodeTag));
            var entity2 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<TestObj>(objForCmpexch.Id, node2inDb.ServerStore.NodeTag));

            Assert.NotNull(entity1);
            Assert.NotNull(entity1.Value);
            Assert.NotNull(entity2);
            Assert.NotNull(entity2.Value);
            Assert.Equal(objForCmpexch.Prop, entity1.Value.Prop);
            Assert.Equal(entity1.Value.Prop, entity2.Value.Prop);
        }

        private async Task WaitAndAssertRehabs(DocumentStore store, List<string> expectedRehabs)
        {
            Assert.True(await WaitUntilDatabaseHasState(store, timeout: TimeSpan.FromSeconds(15), predicate: record =>
            {
                var rehabs = record?.Topology?.Rehabs;
                return rehabs != null && rehabs.Count == expectedRehabs.Count && ContainsAll(rehabs, expectedRehabs);
            }), "Rehabs are not as expected");
        }

        private async Task WaitAndAssertMembers(DocumentStore store, List<string> expectedMembers)
        {
            Assert.True(await WaitUntilDatabaseHasState(store, timeout: TimeSpan.FromSeconds(15), predicate: record =>
            {
                var members = record?.Topology?.Members;
                return members != null && members.Count == expectedMembers.Count && ContainsAll(members, expectedMembers);
            }), "Members are not as expected");
        }

        private static void DisconnectNode(List<RavenServer> nodes, string nodeTag)
        {
            foreach (var node in nodes)
            {
                node.ServerStore.Engine.ForTestingPurposes.NodeTagsToDisconnect.Add(nodeTag);
            }
        }

        private static void ReconnectNode(List<RavenServer> nodes, string nodeTag)
        {
            foreach (var node in nodes)
            {
                if (node.ServerStore.Engine.ForTestingPurposes.NodeTagsToDisconnect.Remove(nodeTag) == false)
                {
                    throw new InvalidOperationException($"Node '{nodeTag}' was already connected to node '{node.ServerStore.NodeTag}");
                }
            }
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

        private class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }
    }
}
