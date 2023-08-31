using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Server.Config.Categories;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Maintenance.Sharding
{
    internal sealed class OrchestratorTopologyUpdater : DatabaseTopologyUpdater
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
            var lastCommittedIndex = _engine.GetLastCommitIndex(context);
            
            if (nodeStats.ServerReport.LastCommittedIndex == null)
            {
                _logger.Log($"Expected to get the Last Committed Index in the node's server report but it is empty", state.ObserverIteration);
                return (false, null);
            }

            if (nodeStats.ServerReport.LastCommittedIndex < lastCommittedIndex)
            {
                var msg = $"Node {promotable} is not ready to be promoted to orchestrator. Not all cluster commands finished applying." +
                          Environment.NewLine +
                          $"Last Committed Cluster Raft Index: {nodeStats.ServerReport.LastCommittedIndex}" + Environment.NewLine +
                          $"Leader's Last Committed Raft Index: {lastCommittedIndex}";

                _logger.Log($"Node {promotable} hasn't been promoted because its last commit index isn't up to date yet", state.ObserverIteration);

                if (state.DatabaseTopology.DemotionReasons.TryGetValue(promotable, out var demotionReason) == false ||
                    msg.Equals(demotionReason) == false)
                {
                    state.DatabaseTopology.DemotionReasons[promotable] = msg;
                    state.DatabaseTopology.PromotablesStatus[promotable] = DatabasePromotionStatus.RaftIndexNotUpToDate;
                    return (false, msg);
                }

                return (false, null);
            }

            state.DatabaseTopology.DemotionReasons.Remove(promotable);
            state.DatabaseTopology.PromotablesStatus.Remove(promotable);
            return (true, $"Node {promotable} is ready to be promoted to orchestrator");
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
