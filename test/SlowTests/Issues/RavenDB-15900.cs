using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Runtime.Intrinsics.X86;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace SlowTests.Issues
{
    public class RavenDB_15900 : ReplicationTestBase
    {
        public RavenDB_15900(ITestOutputHelper output) : base(output)
        {
        }

        public class TestCommandValidator : RachisVersionValidation
        {
            public override void AssertPutCommandToLeader(CommandBase cmd)
            {
            }

            public override void AssertEntryBeforeSendToFollower(BlittableJsonReaderObject entry, int version, string follower)
            {
            }
        }

        [Fact]
        public async Task RemoveEntryFromRaftLogEP()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);

            await ActionWithLeader(l => l.ServerStore.Engine.StateMachine.Validator = new TestCommandValidator());

            using var store = GetDocumentStore(new Options() { Server = leader, ReplicationFactor = 1 });

            // Stuck leader on this command

            var testCmd = new RachisConsensusTestBase.TestCommandWithRaftId("test", RaftIdGenerator.NewId()); 
            await Assert.ThrowsAsync<UnknownClusterCommandException>(() => leader.ServerStore.Engine.CurrentLeader.PutAsync(testCmd, TimeSpan.FromSeconds(2)));

            // Get last raft index from leader
            var testCmdIndex = await Cluster.WaitForRaftCommandToBeAppendedInClusterAsync(nodes, nameof(RachisConsensusTestBase.TestCommandWithRaftId));
            Assert.NotEqual(testCmdIndex, -1);

            // Wait for all nodes to be updated to leader last raft index
            var lastNotifiedIndex = leader.ServerStore.Engine.StateMachine.LastNotifiedIndex;
            await AssertRaftIndexToBeUpdatedOnNodesAsync(lastNotifiedIndex, nodes);

            var database = GetDatabaseName();
            _ = leader.ServerStore.SendToLeaderAsync(new AddDatabaseCommand(Guid.NewGuid().ToString())
            {
                Record = new DatabaseRecord(database)
                {
                    Topology = new DatabaseTopology { Members = new List<string> { "A", "B", "C" }, Rehabs = new List<string> { }, ReplicationFactor = 3 }
                },
                Name = database
            });

            foreach (var server in Servers)
            {
                Assert.False(server.ServerStore.DatabasesLandlord.IsDatabaseLoaded(database));
            }

            List<string> nodelist = new List<string>();
            var nodesCount = await WaitForValueAsync(async () =>
            {
                nodelist = await store.Maintenance.SendAsync(new RemoveEntryFromRaftLogOperation(testCmdIndex));
                return nodelist.Count;
            }, 3);
            Assert.Equal(3, nodesCount);


            foreach (var server in Servers)
            {
                long lastCommittedIndex = 0;
                var res = await WaitForValueAsync(() =>
                {
                    using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        server.ServerStore.Engine.GetLastCommitIndex(context, out lastCommittedIndex, out long _);
                    }
                    return lastCommittedIndex > testCmdIndex;
                }, true);
                Assert.True(res, $"State machine is stuck. raft index was {testCmdIndex}, after remove raft entry index is {lastCommittedIndex} ");
            }

            foreach (var server in Servers)
            {
                var val = await WaitForValueAsync(() => server.ServerStore.DatabasesLandlord.IsDatabaseLoaded(database), true);
                Assert.True(val);

                Assert.Contains(server.ServerStore.NodeTag, nodelist);
            }
        }


        private class RemoveEntryFromRaftLogOperation : IMaintenanceOperation<List<string>>
        {
            private readonly long _index;

            public RemoveEntryFromRaftLogOperation(long index)
            {
                _index = index;
            }

            public RavenCommand<List<string>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new RemoveEntryFromRaftCommand(_index);
            }

            private class RemoveEntryFromRaftCommand : RavenCommand<List<string>>
            {
                private readonly long _index;

                public RemoveEntryFromRaftCommand(long index)
                {
                    _index = index;
                }

                public override bool IsReadRequest => false;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/admin/cluster/remove-entry-from-log?index={_index}";

                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Post
                    };
                }

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    Result = new List<string>();

                    response.TryGet("Nodes", out BlittableJsonReaderArray array);

                    foreach (var item in array)
                        Result.Add(item.ToString());
                }
            }
        }

        [Fact]
        public async Task RemoveEntryFromRaftLogTest()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);

            await ActionWithLeader(l => l.ServerStore.Engine.StateMachine.Validator = new TestCommandValidator());

            // Stuck leader on this command
            var testCmd = new RachisConsensusTestBase.TestCommandWithRaftId("test", RaftIdGenerator.NewId());
            await Assert.ThrowsAsync<UnknownClusterCommandException>(() => leader.ServerStore.Engine.CurrentLeader.PutAsync(testCmd, TimeSpan.FromSeconds(2)));

            // Get last raft index from leader
            var testCmdIndex = await Cluster.WaitForRaftCommandToBeAppendedInClusterAsync(nodes, nameof(RachisConsensusTestBase.TestCommandWithRaftId));
            Assert.NotEqual(testCmdIndex, -1);

            // Wait for all nodes to be updated to leader last raft index
            var lastNotifiedIndex = leader.ServerStore.Engine.StateMachine.LastNotifiedIndex;
            await AssertRaftIndexToBeUpdatedOnNodesAsync(lastNotifiedIndex, nodes);

            var database = GetDatabaseName();
            _ = leader.ServerStore.SendToLeaderAsync(new AddDatabaseCommand(Guid.NewGuid().ToString())
            {
                Record = new DatabaseRecord(database)
                {
                    Topology = new DatabaseTopology { Members = new List<string> { "A", "B", "C" }, Rehabs = new List<string> { }, ReplicationFactor = 3 }
                },
                Name = database
            });

            foreach (var server in Servers)
            {
                Assert.False(server.ServerStore.DatabasesLandlord.IsDatabaseLoaded(database));
            }

            foreach (var server in Servers)
            {
                var res = await WaitForValueAsync(() => server.ServerStore.Engine.RemoveEntryFromRaftLogAsync(testCmdIndex), true);

                Assert.True(res);
            }

            
            foreach (var server in Servers)
            {
                long lastCommittedIndex = 0;
                var res = await WaitForValueAsync(() =>
                {
                    using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        server.ServerStore.Engine.GetLastCommitIndex(context, out lastCommittedIndex, out long _);
                    }
                    return lastCommittedIndex > testCmdIndex;
                }, true);
                Assert.True(res, $"State machine is stuck. raft index was {testCmdIndex}, after remove raft entry index is {lastCommittedIndex} ");
            }

            foreach (var server in Servers)
            {
                var val = await WaitForValueAsync(() => server.ServerStore.DatabasesLandlord.IsDatabaseLoaded(database), true);
                Assert.True(val);
            }

        }

        public async Task AssertRaftIndexToBeUpdatedOnNodesAsync(long index, List<RavenServer> nodes, int timeout = 15000, int interval = 100)
        {
            var sw = Stopwatch.StartNew();

            var nodeTags = new List<string>();
            var updated = false;
            while (sw.ElapsedMilliseconds < timeout)
            {
                nodeTags = nodes
                    .Where(node => node.ServerStore.Engine.StateMachine.LastNotifiedIndex < index)
                    .Select(node => node.ServerStore.NodeTag).ToList();
                if (nodeTags.Count == 0)
                {
                    updated = true;
                    break;
                }
                await Task.Delay(interval);
            }
            Assert.True(updated, $"Nodes {string.Join(" ", nodeTags)} are not updated to the index {index}");
        }

    }
}
