// -----------------------------------------------------------------------
//  <copyright file="ClusterBasic.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rachis.Transport;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Database.Raft;
using Raven.Database.Raft.Dto;
using Raven.Database.Raft.Util;
using Raven.Json.Linq;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Raft
{
    public class SnaphshottingTest : RaftTestBase
    {
        [Fact]
        public void CanInstallSnapshot()
        {
            CreateRaftCluster(3, inMemory:false); // 3 nodes

            for (var i = 0; i < 5; i++)
            {
                var client = servers[0].Options.ClusterManager.Value.Client;
                client.SendClusterConfigurationAsync(new ClusterConfiguration {EnableReplication = false}).Wait(3000);
            }

            var leader = servers.FirstOrDefault(server => server.Options.ClusterManager.Value.IsLeader());
            Assert.NotNull(leader);

            var newServer = GetNewServer(GetPort(), runInMemory: false);

            var snapshotInstalledMre = new ManualResetEventSlim();

            newServer.Options.ClusterManager.Value.Engine.SnapshotInstalled += () => snapshotInstalledMre.Set();

            var allNodesFinishedJoining = new ManualResetEventSlim();
            leader.Options.ClusterManager.Value.Engine.TopologyChanged += command =>
            {
                if (command.Requested.AllNodeNames.All(command.Requested.IsVoter))
                {
                    allNodesFinishedJoining.Set();
                }
            };

            Assert.True(leader.Options.ClusterManager.Value.Engine.AddToClusterAsync(new NodeConnectionInfo
            {
                Name = RaftHelper.GetNodeName(newServer.SystemDatabase.TransactionalStorage.Id),
                Uri = RaftHelper.GetNodeUrl(newServer.SystemDatabase.Configuration.ServerUrl)
            }).Wait(20000));
            Assert.True(allNodesFinishedJoining.Wait(20000));

            Assert.True(snapshotInstalledMre.Wait(TimeSpan.FromSeconds(5)));
        }

        protected override void ModifyServer(RavenDbServer ravenDbServer)
        {
            ravenDbServer.Options.ClusterManager.Value.Engine.Options.MaxLogLengthBeforeCompaction = 4;
        }
    }
}
