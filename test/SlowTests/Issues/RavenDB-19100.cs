using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using NuGet.ContentModel;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace SlowTests.Issues
{
    public class RavenDB_19100 : ClusterTestBase
    {
        public RavenDB_19100(ITestOutputHelper output) : base(output)
        {
            DoNotReuseServer();
        }

        [Fact]
        public async Task UpdateDbTopologyOperation_RemoveMember()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            var storeOptions = new Options { Server = leader, ReplicationFactor = nodes.Count };

            using (var store = GetDocumentStore(storeOptions))
            {
                var databaseName = store.Database;
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                var databaseTopology = record.Topology;

                // Modify databaseTopology
                databaseTopology.ReplicationFactor--;
                databaseTopology.Members.Remove(databaseTopology.Members.First());

                // Execute command
                var res = await store.Maintenance.Server.SendAsync(new ModifyDatabaseTopologyOperation(databaseName, databaseTopology));

                // Wait for command (raft index)
                await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(res.RaftCommandIndex, nodes);

                // Check equality of new record topology and databaseTopology
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                Assert.Equal(databaseTopology.ReplicationFactor, record.Topology.ReplicationFactor);
                Assert.Equal(databaseTopology.Members.Count, record.Topology.Members.Count);
                foreach (var member in databaseTopology.Members)
                {
                    Assert.True(record.Topology.Members.Contains(member), "Members in topologies are not equals");
                }
            }
        }

        [Fact]
        public async Task UpdateDbTopologyOperationTest_TurnMemberIntoRehab()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            Cluster.SuspendObserver(leader);
            var storeOptions = new Options
            {
                Server = leader,
                ReplicationFactor = nodes.Count
            };

            using (var store = GetDocumentStore(storeOptions))
            {
                var databaseName = store.Database;
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                var databaseTopology = record.Topology;

                // Modify databaseTopology
                var node = databaseTopology.Members.First();
                databaseTopology.Members.Remove(node);
                databaseTopology.Rehabs.Add(node);

                // Execute command
                var res = await store.Maintenance.Server.SendAsync(new ModifyDatabaseTopologyOperation(databaseName, databaseTopology));

                // Wait for command (raft index)
                await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(res.RaftCommandIndex, nodes);

                // Check equality of new record topology and databaseTopology
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                Assert.Equal(databaseTopology.ReplicationFactor, record.Topology.ReplicationFactor);
                Assert.Equal(databaseTopology.Members.Count, record.Topology.Members.Count);
                foreach (var member in databaseTopology.Members)
                { 
                    Assert.True(record.Topology.Members.Contains(member), "Members in topologies are not equals");
                }
                Assert.Equal(databaseTopology.Rehabs.Count, record.Topology.Rehabs.Count);
                foreach (var rehab in databaseTopology.Rehabs)
                {
                    Assert.True(record.Topology.Rehabs.Contains(rehab), "Rehabs in topologies are not equals");
                }
            }
        }
    }
}
