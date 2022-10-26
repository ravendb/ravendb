using System;
using Raven.Client.ServerWide;
using Raven.Server.Config.Categories;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.ServerWide.Maintenance.Sharding
{
    internal class OrchestratorTopologyUpdater : DatabaseTopologyUpdater
    {
        public OrchestratorTopologyUpdater(ServerStore server,
            RachisConsensus<ClusterStateMachine> engine,
            ClusterConfiguration clusterConfiguration,
            DateTime clusterObserverStartTime,
            LogMessageDel logMessage) : base(server, engine, clusterConfiguration, clusterObserverStartTime, logMessage)
        {
        }

        protected override (bool Promote, string UpdateTopologyReason) TryGetMentorAndPromote(ClusterOperationContext context, ClusterObserver.DatabaseObservationState state, string promotable)
        {
            return (true, $"Node {promotable} is ready to be promoted to orchestrator");
        }

        protected override bool IsLastCommittedIndexCaughtUp(ClusterOperationContext context, string node, DatabaseTopology topology, ClusterNodeStatusReport nodeStats)
        {
            var lastCommittedIndex = _engine.GetLastCommitIndex(context);

            //heartbeat legacy
            if (nodeStats.ServerReport.LastCommittedIndex == null)
            {
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Pawel, DevelopmentHelper.Severity.Normal, "Should handle mixed cluster of 5.x and 6?");
            }

            if (nodeStats.ServerReport.LastCommittedIndex < lastCommittedIndex)
            {
                var msg = $"Node {node} is not ready to be promoted to orchestrator. Not all cluster transactions finished applying." +
                          Environment.NewLine +
                          $"Last Committed Cluster Raft Index: {lastCommittedIndex}" + Environment.NewLine +
                          $"Leader's Last Completed Cluster Transaction Raft Index: {nodeStats.ServerReport.LastCommittedIndex}";

                LogMessage($"Node {node} hasn't been promoted because its last commit index isn't up to date yet");

                if (topology.DemotionReasons.TryGetValue(node, out var demotionReason) == false ||
                    msg.Equals(demotionReason) == false)
                {
                    topology.DemotionReasons[node] = msg;
                    topology.PromotablesStatus[node] = DatabasePromotionStatus.RaftIndexNotUpToDate;
                    return false;
                }

                return false;
            }

            return true;
        }
    }
}
