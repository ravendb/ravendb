// -----------------------------------------------------------------------
//  <copyright file="ClusterBasic.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Raven.Client.Document;
using Raven.Database.Raft.Util;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Raft
{
    public class ClusterLeave : RaftTestBase
    {
        private List<DocumentStore> clusterStores;

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        public void CanLeaveLeaderFromClusterFromLeader(int nodesCount)
        {
            clusterStores = CreateRaftCluster(nodesCount);
            var leaderIndex = servers.FindIndex(server => server.Options.ClusterManager.Value.IsLeader());
            CanLeaveNodeFromNode(leaderIndex, leaderIndex, nodesCount);
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        public void CanLeaveLeaderFromClusterFromNonLeader(int nodesCount)
        {
            clusterStores = CreateRaftCluster(nodesCount);
            var leaderIndex = servers.FindIndex(server => server.Options.ClusterManager.Value.IsLeader());
            CanLeaveNodeFromNode(leaderIndex, (leaderIndex + 1) % nodesCount, nodesCount);
        }


        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        public void CanLeaveNonLeaderFromClusterFromLeader(int nodesCount)
        {
            clusterStores = CreateRaftCluster(nodesCount);
            var leaderIndex = servers.FindIndex(server => server.Options.ClusterManager.Value.IsLeader());
            CanLeaveNodeFromNode((leaderIndex + 1) % nodesCount, leaderIndex, nodesCount);
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        public void CanLeaveNonLeaderFromClusterFromNonLeader(int nodesCount)
        {
            clusterStores = CreateRaftCluster(nodesCount);
            var leaderIndex = servers.FindIndex(server => server.Options.ClusterManager.Value.IsLeader());
            CanLeaveNodeFromNode((leaderIndex + 2) % nodesCount, (leaderIndex + 1) % nodesCount, nodesCount);
        }

        private void CanLeaveNodeFromNode(int nodeToRemoveIndex, int nodeToInitializeLeaveIndex, int nodesCount)
        {
            var clientUsedForSendingRequest = clusterStores[nodeToInitializeLeaveIndex];
            var nodeAboutToRemove = servers[nodeToRemoveIndex];

            var guidOfNodeToRemove = servers[nodeToRemoveIndex].Options.ClusterManager.Value.Engine.Name;
            clientUsedForSendingRequest.DatabaseCommands.ForSystemDatabase().CreateRequest("/admin/cluster/leave?name=" + guidOfNodeToRemove, new HttpMethod("GET")).ExecuteRequest();

            // validate if removed node doesn't exist in new topology
            foreach (var server in servers)
            {
                if (server == nodeAboutToRemove)
                {
                    // validate if topology on node which just leaved cluster contains single element
                    var newTopologyOfRemovedNode = nodeAboutToRemove.Options.ClusterManager.Value.Engine.CurrentTopology;
                    Assert.Equal(1, newTopologyOfRemovedNode.AllNodes.Count());
                }
                else
                {
                    var currentTopology = server.Options.ClusterManager.Value.Engine.CurrentTopology;
                    Assert.Equal(nodesCount - 1, currentTopology.AllNodes.Count());
                }
            }
        }
    }
}
