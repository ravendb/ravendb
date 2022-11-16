using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Server.Config.Categories;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
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
            ObserverLogger logger) : base(server, engine, clusterConfiguration, clusterObserverStartTime, logger)
        {
        }

        protected override (bool Promote, string UpdateTopologyReason) TryPromote(ClusterOperationContext context, ClusterObserver.DatabaseObservationState state, string promotable, ClusterNodeStatusReport nodeStats)
        {
            if (IsLastCommittedIndexCaughtUp(context, promotable, state.DatabaseTopology, nodeStats, state.ObserverIteration))
            {
                state.DatabaseTopology.PromotablesStatus.Remove(promotable);
                state.DatabaseTopology.DemotionReasons.Remove(promotable);
                return (true, $"Node {promotable} is ready to be promoted to orchestrator");
            }

            return (false, $"Node {promotable} is not ready to be promoted to orchestrator because its index is not caught up yet");
        }

        private bool IsLastCommittedIndexCaughtUp(ClusterOperationContext context, string node, DatabaseTopology topology, ClusterNodeStatusReport nodeStats, long iteration)
        {
            var lastCommittedIndex = _engine.GetLastCommitIndex(context);
            
            if (nodeStats.ServerReport.LastCommittedIndex == null)
            {
                _logger.Log($"Expected to get the Last Committed Index in the node's server report but it is empty", iteration);
                return false;
            }

            if (nodeStats.ServerReport.LastCommittedIndex < lastCommittedIndex)
            {
                var msg = $"Node {node} is not ready to be promoted to orchestrator. Not all cluster transactions finished applying." +
                          Environment.NewLine +
                          $"Last Committed Cluster Raft Index: {lastCommittedIndex}" + Environment.NewLine +
                          $"Leader's Last Completed Cluster Transaction Raft Index: {nodeStats.ServerReport.LastCommittedIndex}";

                _logger.Log($"Node {node} hasn't been promoted because its last commit index isn't up to date yet", iteration);

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

        protected override void RemoveOtherNodesIfNeeded(ClusterObserver.DatabaseObservationState state, ref List<DeleteDatabaseCommand> deletions)
        {
            if (state.DatabaseTopology.Members.Count < state.DatabaseTopology.ReplicationFactor)
                return;

            if (state.DatabaseTopology.Promotables.Count == 0 &&
                state.DatabaseTopology.Rehabs.Count == 0)
                return;

            var nonMembers = state.DatabaseTopology.Promotables.Concat(state.DatabaseTopology.Rehabs).ToList();
            foreach (var node in nonMembers)
            {
                state.DatabaseTopology.RemoveFromTopology(node);
            }
        }
    }
}
