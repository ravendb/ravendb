using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Server.Extensions;
using Sparrow.Utils;
using static Raven.Server.ServerWide.Maintenance.ClusterObserver;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.ServerWide.Maintenance
{
    internal class DatabaseTopologyUpdater
    {
        private readonly ServerStore _server;
        protected readonly RachisConsensus<ClusterStateMachine> _engine;
        private readonly bool _hardDeleteOnReplacement;
        private readonly DateTime _observerStartTime;
        protected readonly ObserverLogger _logger;
        private readonly long _moveToRehabTimeMs;
        private readonly long _maxChangeVectorDistance;
        private readonly long _rotateGraceTimeMs;
        private readonly TimeSpan _breakdownTimeout;
        private readonly TimeSpan _supervisorSamplePeriod;

        public DatabaseTopologyUpdater(ServerStore server,
            RachisConsensus<ClusterStateMachine> engine,
            ClusterConfiguration clusterConfiguration,
            DateTime clusterObserverStartTime,
            ObserverLogger logger)
        {
            _server = server;
            _engine = engine;
            _hardDeleteOnReplacement = clusterConfiguration.HardDeleteOnReplacement;
            _observerStartTime = clusterObserverStartTime;
            _logger = logger;
            _moveToRehabTimeMs = (long)clusterConfiguration.MoveToRehabGraceTime.AsTimeSpan.TotalMilliseconds;
            _maxChangeVectorDistance = clusterConfiguration.MaxChangeVectorDistance;
            _rotateGraceTimeMs = (long)clusterConfiguration.RotatePreferredNodeGraceTime.AsTimeSpan.TotalMilliseconds;
            _breakdownTimeout = clusterConfiguration.AddReplicaTimeout.AsTimeSpan;
            _supervisorSamplePeriod = clusterConfiguration.SupervisorSamplePeriod.AsTimeSpan;
        }

        public string Update(ClusterOperationContext context, DatabaseObservationState state, ref List<DeleteDatabaseCommand> deletions)
        {
            var hasLivingNodes = false;

            var databaseTopology = state.DatabaseTopology;
            var current = state.Current;
            var previous = state.Previous;
            var dbName = state.Name;

            var someNodesRequireMoreTime = false;
            var rotatePreferredNode = false;

            // handle legacy commands
            if (databaseTopology.NodesModifiedAt == null ||
                databaseTopology.NodesModifiedAt == DateTime.MinValue)
            {
                return "Adding last modification to legacy database";
            }

            foreach (var member in databaseTopology.Members)
            {
                var status = DatabaseStatus.None;

                if (current.TryGetValue(member, out var nodeStats) == false)
                {
                    // there isn't much we can do here, except for log it.
                    if (previous.TryGetValue(member, out _))
                    {
                        // if we found this node in the previous report, we will ignore it this time and wait for the next report.
                        continue;
                    }

                    var msg =
                        $"The member node {member} was not found in both current and previous reports of the cluster observer. " +
                        $"If this error continue to raise, check the latency between the cluster nodes.";
                    _logger.Log(msg, state.ObserverIteration, database: dbName);
                    RaiseNodeNotFoundAlert(msg, member, state.ObserverIteration);
                    continue;
                }

                DatabaseStatusReport dbStats = null;
                if (nodeStats.Status == ClusterNodeStatusReport.ReportStatus.Ok &&
                    nodeStats.Report.TryGetValue(dbName, out dbStats))
                {
                    status = dbStats.Status;
                    if (status == DatabaseStatus.Loaded ||
                        status == DatabaseStatus.Unloaded ||
                        status == DatabaseStatus.NoChange)
                    {
                        hasLivingNodes = true;

                        if (databaseTopology.PromotablesStatus.TryGetValue(member, out _) ||
                            databaseTopology.DemotionReasons.TryGetValue(member, out _))
                        {
                            databaseTopology.DemotionReasons.Remove(member);
                            databaseTopology.PromotablesStatus.Remove(member);
                            return $"Node {member} is online";
                        }
                        continue;
                    }
                }

                if (_server.DatabasesLandlord.ForTestingPurposes?.HoldDocumentDatabaseCreation != null)
                    _server.DatabasesLandlord.ForTestingPurposes.PreventedRehabOfIdleDatabase = true;

                if (ShouldGiveMoreTimeBeforeMovingToRehab(nodeStats.LastSuccessfulUpdateDateTime, dbStats?.UpTime))
                {
                    if (ShouldGiveMoreTimeBeforeRotating(nodeStats.LastSuccessfulUpdateDateTime, dbStats?.UpTime) == false)
                    {
                        // It seems that the node has some trouble.
                        // We will give him more time before moving to rehab, but we need to make sure he isn't the preferred node.
                        if (databaseTopology.Members.Count > 1 &&
                            databaseTopology.Members[0] == member)
                        {
                            rotatePreferredNode = true;
                        }
                    }

                    someNodesRequireMoreTime = true;
                    continue;
                }

                if (TryMoveToRehab(dbName, databaseTopology, current, member, state.ObserverIteration))
                    return $"Node {member} is currently not responding (with status: {status}) and moved to rehab ({DateTime.UtcNow - nodeStats.LastSuccessfulUpdateDateTime})";

                // database distribution is off and the node is down
                if (databaseTopology.DynamicNodesDistribution == false && (
                        databaseTopology.PromotablesStatus.TryGetValue(member, out var currentStatus) == false
                        || currentStatus != DatabasePromotionStatus.NotResponding))
                {
                    databaseTopology.DemotionReasons[member] = "Not responding";
                    databaseTopology.PromotablesStatus[member] = DatabasePromotionStatus.NotResponding;
                    return $"Node {member} is currently not responding with the status '{status}'";
                }
            }

            if (hasLivingNodes && rotatePreferredNode)
            {
                var member = databaseTopology.Members[0];
                databaseTopology.Members.Remove(member);
                databaseTopology.Members.Add(member);
                return $"The preferred Node {member} is currently not responding and moved to the end of the list";
            }

            if (hasLivingNodes == false)
            {
                var recoverable = new List<string>();

                foreach (var rehab in databaseTopology.Rehabs)
                {
                    if (FailedDatabaseInstanceOrNode(rehab, state, out _) == DatabaseHealth.Good)
                        recoverable.Add(rehab);
                }

                if (databaseTopology.Members.Count == 0)
                {
                    // as last resort we will promote a promotable
                    foreach (var promotable in databaseTopology.Promotables)
                    {
                        if (FailedDatabaseInstanceOrNode(promotable, state, out _) == DatabaseHealth.Good)
                            recoverable.Add(promotable);
                    }
                }

                if (recoverable.Count > 0)
                {
                    var node = FindMostUpToDateNode(recoverable, dbName, current);
                    databaseTopology.Rehabs.Remove(node);
                    databaseTopology.Promotables.Remove(node);
                    databaseTopology.Members.Add(node);

                    RaiseNoLivingNodesAlert($"None of '{dbName}' database nodes are responding to the supervisor, promoting {node} to avoid making the database completely unreachable.", dbName, state.ObserverIteration);
                    return $"None of '{dbName}' nodes are responding, promoting {node}";
                }

                if (state.RawDatabase.EntireDatabasePendingDeletion())
                {
                    return null; // We delete the whole database.
                }

                RaiseNoLivingNodesAlert($"None of '{dbName}' database nodes are responding to the supervisor, the database is unreachable.", dbName, state.ObserverIteration);
            }

            if (someNodesRequireMoreTime == false)
            {
                if (CheckMembersDistance(state, out string reason) == false)
                    return reason;

                if (databaseTopology.TryUpdateByPriorityOrder())
                    return "Reordering the member nodes to ensure the priority order.";
            }

            var shouldUpdateTopologyStatus = false;
            var updateTopologyStatusReason = new StringBuilder();

            foreach (var promotable in databaseTopology.Promotables)
            {
                if (FailedDatabaseInstanceOrNode(promotable, state, out var nodeStats) == DatabaseHealth.Bad)
                {
                    // database distribution is off and the node is down
                    if (databaseTopology.DynamicNodesDistribution == false)
                    {
                        if (databaseTopology.PromotablesStatus.TryGetValue(promotable, out var currentStatus) == false
                            || currentStatus != DatabasePromotionStatus.NotResponding)
                        {
                            databaseTopology.DemotionReasons[promotable] = "Not responding";
                            databaseTopology.PromotablesStatus[promotable] = DatabasePromotionStatus.NotResponding;
                            return $"Node {promotable} is currently not responding";
                        }
                        continue;
                    }

                    if (TryFindFitNode(promotable, state, state.DatabaseTopology, state.ClusterTopology, state.Current, state.Name, out var node) == false)
                    {
                        if (databaseTopology.PromotablesStatus.TryGetValue(promotable, out var currentStatus) == false
                            || currentStatus != DatabasePromotionStatus.NotResponding)
                        {
                            databaseTopology.DemotionReasons[promotable] = "Not responding";
                            databaseTopology.PromotablesStatus[promotable] = DatabasePromotionStatus.NotResponding;
                            return $"Node {promotable} is currently not responding";
                        }
                        continue;
                    }

                    if (_server.LicenseManager.CanDynamicallyDistributeNodes(withNotification: false, out _) == false)
                        continue;

                    // replace the bad promotable otherwise we will continue to add more and more nodes.
                    databaseTopology.Promotables.Add(node);
                    databaseTopology.DemotionReasons[node] = $"Just replaced the promotable node {promotable}";
                    databaseTopology.PromotablesStatus[node] = DatabasePromotionStatus.WaitingForFirstPromotion;
                    var deletionCmd = new DeleteDatabaseCommand(dbName, RaftIdGenerator.NewId())
                    {
                        ErrorOnDatabaseDoesNotExists = false,
                        FromNodes = new[] { promotable },
                        HardDelete = _hardDeleteOnReplacement,
                        UpdateReplicationFactor = false
                    };

                    if (deletions == null)
                        deletions = new List<DeleteDatabaseCommand>();
                    deletions.Add(deletionCmd);
                    return $"The promotable {promotable} is not responsive, replace it with a node {node}";
                }

                var tryPromote = TryPromote(context, state, promotable, nodeStats);

                if (tryPromote.Promote)
                {
                    databaseTopology.Promotables.Remove(promotable);
                    databaseTopology.Members.Add(promotable);
                    databaseTopology.PredefinedMentors.Remove(promotable);
                    RemoveOtherNodesIfNeeded(state, ref deletions);
                    databaseTopology.ReorderMembers();
                    return $"Promoting node {promotable} to member";
                }
                if (tryPromote.UpdateTopologyReason != null)
                {
                    shouldUpdateTopologyStatus = true;
                    updateTopologyStatusReason.AppendLine(tryPromote.UpdateTopologyReason);
                }
            }

            var goodMembers = GetNumberOfRespondingNodes(state);
            var pendingDelete = GetPendingDeleteNodes(state);
            foreach (var rehab in databaseTopology.Rehabs)
            {
                var health = FailedDatabaseInstanceOrNode(rehab, state, out var nodeStats);
                switch (health)
                {
                    case DatabaseHealth.Bad:
                        if (databaseTopology.DynamicNodesDistribution == false)
                            continue;

                        if (goodMembers < databaseTopology.ReplicationFactor &&
                            TryFindFitNode(rehab, state, state.DatabaseTopology, state.ClusterTopology, state.Current, state.Name, out var node))
                        {
                            if (_server.LicenseManager.CanDynamicallyDistributeNodes(withNotification: false, out _) == false)
                                continue;

                            databaseTopology.Promotables.Add(node);
                            databaseTopology.DemotionReasons[node] = $"Maintain the replication factor and create new replica instead of node {rehab}";
                            databaseTopology.PromotablesStatus[node] = DatabasePromotionStatus.WaitingForFirstPromotion;
                            return $"The rehab node {rehab} was too long in rehabilitation, create node {node} to replace it";
                        }

                        if (databaseTopology.PromotablesStatus.TryGetValue(rehab, out var status) == false || status != DatabasePromotionStatus.NotResponding)
                        {
                            // was already online, but now we lost the connection again
                            if (TryMoveToRehab(dbName, databaseTopology, current, rehab, state.ObserverIteration))
                            {
                                return $"Node {rehab} is currently not responding";
                            }
                        }

                        break;

                    case DatabaseHealth.Good:

                        if (pendingDelete.Contains(rehab) && databaseTopology.PromotablesStatus.ContainsKey(rehab) == false)
                        {
                            // already tried to promote, so we just ignore and continue
                            continue;
                        }

                        var tryPromote = TryPromote(context, state, rehab, nodeStats);

                        if (tryPromote.Promote)
                        {
                            _logger.Log($"The database {dbName} on {rehab} is reachable and up to date, so we promote it back to member.", state.ObserverIteration, database: dbName);

                            databaseTopology.Members.Add(rehab);
                            databaseTopology.Rehabs.Remove(rehab);
                            RemoveOtherNodesIfNeeded(state, ref deletions);
                            databaseTopology.ReorderMembers();

                            return $"Node {rehab} was recovered from rehabilitation and promoted back to member";
                        }
                        if (tryPromote.UpdateTopologyReason != null)
                        {
                            shouldUpdateTopologyStatus = true;
                            updateTopologyStatusReason.AppendLine(tryPromote.UpdateTopologyReason);
                        }
                        break;
                }
            }
            RemoveOtherNodesIfNeeded(state, ref deletions);

            if (shouldUpdateTopologyStatus)
            {
                return updateTopologyStatusReason.ToString();
            }

            return null;
        }

        protected virtual (bool Promote, string UpdateTopologyReason) TryPromote(ClusterOperationContext context, DatabaseObservationState state, string promotable, ClusterNodeStatusReport _)
        {
            if (_server.DatabasesLandlord.ForTestingPurposes?.PreventNodePromotion == true)
                return (false, "Preventing node promotion for testing purposes.");

            if (TryGetMentorNode(state.Name, state.DatabaseTopology, state.ClusterTopology, promotable, out var mentorNode) == false)
                return (false, null);

            return TryPromote(context, state, mentorNode, promotable);
        }

        private bool CheckMembersDistance(DatabaseObservationState state, out string reason)
        {
            // check every node pair, and if one of them is lagging behind, move him to rehab
            reason = null;

            if (ShouldGiveMoreTimeBeforeMovingToRehab(state.DatabaseTopology.NodesModifiedAt ?? DateTime.MinValue, databaseUpTime: null))
                return true;

            var members = state.DatabaseTopology.Members;
            for (int i = 0; i < members.Count; i++)
            {
                var member1 = members[i];
                var current1 = state.GetCurrentDatabaseReport(member1);
                var prev1 = state.GetPreviousDatabaseReport(member1);
                if (current1 == null || prev1 == null)
                    continue;

                var myCurrentEtag = current1.LastEtag;
                var myPrevEtag = prev1.LastEtag;

                for (int j = 0; j < members.Count; j++)
                {
                    if (i == j)
                        continue;

                    var member2 = members[j];
                    var current2 = state.GetCurrentDatabaseReport(member2);
                    var prev2 = state.GetPreviousDatabaseReport(member2);
                    if (current2 == null || prev2 == null)
                        continue;

                    if (current1.LastSentEtag.TryGetValue(member2, out var currentLastSentEtag) == false)
                        continue;

                    if (prev1.LastSentEtag.TryGetValue(member2, out var prevLastSentEtag) == false)
                        continue;

                    var prevEtagDistance = myPrevEtag - prevLastSentEtag;
                    var currentEtagDistance = myCurrentEtag - currentLastSentEtag;

                    if (Math.Abs(currentEtagDistance) > _maxChangeVectorDistance &&
                        Math.Abs(prevEtagDistance) > _maxChangeVectorDistance)
                    {
                        // we rely both on the etag and change vector,
                        // because the data may find a path to the node even if the direct connection between them is broken.
                        var currentChangeVectorDistance = ChangeVectorUtils.Distance(current1.DatabaseChangeVector, current2.DatabaseChangeVector);
                        var prevChangeVectorDistance = ChangeVectorUtils.Distance(prev1.DatabaseChangeVector, prev2.DatabaseChangeVector);

                        if (Math.Abs(currentChangeVectorDistance) > _maxChangeVectorDistance &&
                            Math.Abs(prevChangeVectorDistance) > _maxChangeVectorDistance)
                        {
                            var rehab = currentChangeVectorDistance > 0 ? member2 : member1;
                            var rehabCheck = prevChangeVectorDistance > 0 ? member2 : member1;
                            if (rehab != rehabCheck)
                                continue; // inconsistent result, same node must be lagging

                            state.DatabaseTopology.Members.Remove(rehab);
                            state.DatabaseTopology.Rehabs.Add(rehab);
                            reason =
                                $"Node {rehab} for database '{state.Name}' moved to rehab, because he is lagging behind. (distance between {member1} and {member2} is {currentChangeVectorDistance})";
                            state.DatabaseTopology.DemotionReasons[rehab] = $"distance between {member1} and {member2} is {currentChangeVectorDistance}";

                            return false;
                        }
                    }
                }
            }

            return true;
        }

        private bool ShouldGiveMoreTimeBeforeMovingToRehab(DateTime lastSuccessfulUpdate, TimeSpan? databaseUpTime) =>
            ShouldGiveMoreGrace(lastSuccessfulUpdate, databaseUpTime, _moveToRehabTimeMs);

        private bool ShouldGiveMoreTimeBeforeRotating(DateTime lastSuccessfulUpdate, TimeSpan? databaseUpTime) =>
            ShouldGiveMoreGrace(lastSuccessfulUpdate, databaseUpTime, _rotateGraceTimeMs);

        private bool ShouldGiveMoreGrace(DateTime lastSuccessfulUpdate, TimeSpan? databaseUpTime, long graceMs)
        {
            var now = DateTime.UtcNow;
            var observerUptime = (now - _observerStartTime).TotalMilliseconds;

            if (graceMs > observerUptime)
                return true;

            if (databaseUpTime.HasValue) // if this has value, it means that we have a connectivity
            {
                return databaseUpTime.Value.TotalMilliseconds < graceMs;
            }

            var lastUpdate = RavenDateTimeExtensions.Max(lastSuccessfulUpdate, _observerStartTime);
            var graceThreshold = lastUpdate.AddMilliseconds(graceMs);
            return graceThreshold > now;
        }

        private int GetNumberOfRespondingNodes(DatabaseObservationState state)
        {
            var topology = state.DatabaseTopology;

            var goodMembers = topology.Members.Count;
            foreach (var promotable in topology.Promotables)
            {
                if (FailedDatabaseInstanceOrNode(promotable, state, out _) != DatabaseHealth.Bad)
                    goodMembers++;
            }
            foreach (var rehab in topology.Rehabs)
            {
                if (FailedDatabaseInstanceOrNode(rehab, state, out _) != DatabaseHealth.Bad)
                    goodMembers++;
            }
            return goodMembers;
        }

        private bool TryMoveToRehab(string dbName, DatabaseTopology topology, Dictionary<string, ClusterNodeStatusReport> current, string member, long iteration)
        {
            DatabaseStatusReport dbStats = null;
            if (current.TryGetValue(member, out var nodeStats) &&
                nodeStats.Status == ClusterNodeStatusReport.ReportStatus.Ok &&
                nodeStats.Report.TryGetValue(dbName, out dbStats))
            {
                switch (dbStats.Status)
                {
                    case DatabaseStatus.Loaded:
                    case DatabaseStatus.Unloaded:
                    case DatabaseStatus.Shutdown:
                    case DatabaseStatus.NoChange:
                        return false;

                    case DatabaseStatus.None:
                    case DatabaseStatus.Loading:
                    case DatabaseStatus.Faulted:
                        // continue the function
                        break;
                }
            }

            string reason;
            if (nodeStats == null)
            {
                reason = "Node in rehabilitation due to no status report in the latest cluster stats";
            }
            else if (nodeStats.Status != ClusterNodeStatusReport.ReportStatus.Ok)
            {
                switch (nodeStats.Status)
                {
                    case ClusterNodeStatusReport.ReportStatus.Timeout:
                        reason = $"Node in rehabilitation due to timeout reached trying to get stats from node.{Environment.NewLine}";
                        break;

                    case ClusterNodeStatusReport.ReportStatus.OutOfCredits:
                        reason = $"Node in rehabilitation because it ran out of CPU credits.{Environment.NewLine}";
                        break;

                    case ClusterNodeStatusReport.ReportStatus.EarlyOutOfMemory:
                        reason = $"Node in rehabilitation because of early out of memory.{Environment.NewLine}";
                        break;

                    case ClusterNodeStatusReport.ReportStatus.HighDirtyMemory:
                        reason = $"Node in rehabilitation because of high dirty memory.{Environment.NewLine}";
                        break;

                    default:
                        reason = $"Node in rehabilitation due to last report status being '{nodeStats.Status}'.{Environment.NewLine}";
                        break;
                }
            }
            else if (nodeStats.Report.TryGetValue(dbName, out var stats) && stats.Status == DatabaseStatus.Faulted)
            {
                reason = $"In rehabilitation because the DatabaseStatus for this node is {nameof(DatabaseStatus.Faulted)}.{Environment.NewLine}";
            }
            else
            {
                reason = $"In rehabilitation because the node is reachable but had no report about the database (Status: {dbStats?.Status}).{Environment.NewLine}";
            }

            if (nodeStats?.Error != null)
            {
                reason += $". {nodeStats.Error}";
            }
            if (dbStats?.Error != null)
            {
                reason += $". {dbStats.Error}";
            }

            MoveNodeToRehab(topology, member, reason, GetStatus(nodeStats));

            _logger.Log($"Node {member} of database '{dbName}': {reason}", iteration, database: dbName);

            return true;
        }

        private DatabasePromotionStatus GetStatus(ClusterNodeStatusReport nodeStats)
        {
            if (nodeStats != null)
            {
                if (nodeStats.ServerReport.OutOfCpuCredits == true)
                    return DatabasePromotionStatus.OutOfCpuCredits;

                if (nodeStats.ServerReport.EarlyOutOfMemory == true)
                    return DatabasePromotionStatus.EarlyOutOfMemory;

                if (nodeStats.ServerReport.HighDirtyMemory == true)
                    return DatabasePromotionStatus.HighDirtyMemory;
            }

            return DatabasePromotionStatus.NotResponding;
        }

        private void MoveNodeToRehab(DatabaseTopology topology, string member, string reason, DatabasePromotionStatus promotionStatus)
        {
            if (topology.Rehabs.Contains(member) == false)
            {
                topology.Members.Remove(member);

                topology.Rehabs.Add(member);
            }

            topology.DemotionReasons[member] = reason;
            topology.PromotablesStatus[member] = promotionStatus;
        }

        protected bool TryGetMentorNode(string dbName, DatabaseTopology topology, ClusterTopology clusterTopology, string promotable, out string mentorNode)
        {
            var url = clusterTopology.GetUrlFromTag(promotable);
            topology.PredefinedMentors.TryGetValue(promotable, out var mentor);
            var task = new PromotableTask(promotable, url, dbName, mentor);
            mentorNode = topology.WhoseTaskIsIt(_server.Engine.CurrentCommittedState.State, task, null);

            if (mentorNode == null)
            {
                // We are in passive mode and were kicked out of the cluster.
                return false;
            }

            return true;
        }

        private (bool Promote, string UpdateTopologyReason) TryPromote(ClusterOperationContext context, DatabaseObservationState state, string mentorNode, string promotable)
        {
            var dbName = state.Name;
            var topology = state.DatabaseTopology;
            var current = state.Current;
            var previous = state.Previous;

            if (previous.TryGetValue(mentorNode, out var mentorPrevClusterStats) == false ||
                mentorPrevClusterStats.Report.TryGetValue(dbName, out var mentorPrevDbStats) == false)
            {
                _logger.Log($"Can't find previous mentor {mentorNode} stats for node {promotable}", state.ObserverIteration, database: dbName);
                return (false, null);
            }

            if (previous.TryGetValue(promotable, out var promotablePrevClusterStats) == false ||
                promotablePrevClusterStats.Report.TryGetValue(dbName, out var promotablePrevDbStats) == false)
            {
                _logger.Log($"Can't find previous stats for node {promotable}", state.ObserverIteration, database: dbName);
                return (false, null);
            }

            if (current.TryGetValue(mentorNode, out var mentorCurrClusterStats) == false ||
                mentorCurrClusterStats.Report.TryGetValue(dbName, out var mentorCurrDbStats) == false)
            {
                _logger.Log($"Can't find current mentor {mentorNode} stats for node {promotable}", state.ObserverIteration, database: dbName);
                return (false, null);
            }

            if (current.TryGetValue(promotable, out var promotableClusterStats) == false ||
                promotableClusterStats.Report.TryGetValue(dbName, out var promotableDbStats) == false)
            {
                _logger.Log($"Can't find current stats for node {promotable}", state.ObserverIteration, database: dbName);
                return (false, null);
            }

            if (promotableClusterStats.ServerReport.OutOfCpuCredits == true)
            {
                _logger.Log($"Can't promote node {promotable}, it doesn't have enough CPU credits", state.ObserverIteration, database: dbName);
                return (false, null);
            }

            if (promotableClusterStats.ServerReport.EarlyOutOfMemory == true)
            {
                _logger.Log($"Can't promote node {promotable}, it's in an early out of memory state", state.ObserverIteration, database: dbName);
                return (false, null);
            }

            if (promotableClusterStats.ServerReport.HighDirtyMemory == true)
            {
                _logger.Log($"Can't promote node {promotable}, it's in high dirty memory state", state.ObserverIteration, database: dbName);
                return (false, null);
            }

            if (topology.Members.Count == topology.ReplicationFactor)
            {
                _logger.Log($"Replication factor is reached", state.ObserverIteration, database: dbName);
                return (false, null);
            }

            var mentorsEtag = mentorPrevDbStats.LastEtag;
            if (mentorCurrDbStats.LastSentEtag.TryGetValue(promotable, out var lastSentEtag) == false)
            {
                _logger.Log($"Can't find last sent etag of mentor {mentorNode} for {promotable}", state.ObserverIteration, database: dbName);
                return (false, null);
            }

            var timeDiff = mentorCurrClusterStats.LastSuccessfulUpdateDateTime - mentorPrevClusterStats.LastSuccessfulUpdateDateTime > 3 * _supervisorSamplePeriod;

            if (lastSentEtag < mentorsEtag || timeDiff)
            {
                var msg = $"The database '{dbName}' on {promotable} not ready to be promoted, because the mentor hasn't sent all of the documents yet." + Environment.NewLine +
                          $"Last sent Etag: {lastSentEtag:#,#;;0}" + Environment.NewLine +
                          $"Mentor's Etag: {mentorsEtag:#,#;;0}";

                _logger.Log($"Mentor {mentorNode} hasn't sent all of the documents yet to {promotable} (time diff: {timeDiff}, sent etag: {lastSentEtag:#,#;;0}/{mentorsEtag:#,#;;0})", state.ObserverIteration, database: dbName);

                if (topology.DemotionReasons.TryGetValue(promotable, out var demotionReason) == false ||
                    msg.Equals(demotionReason) == false)
                {
                    topology.DemotionReasons[promotable] = msg;
                    topology.PromotablesStatus[promotable] = DatabasePromotionStatus.ChangeVectorNotMerged;
                    return (false, msg);
                }
                return (false, null);
            }

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Check if we're getting the proper compare exchange for shard databases");
#pragma warning disable CS0618
            var leaderLastCompareExchangeIndex = _server.Cluster.GetLastCompareExchangeIndexForDatabase(context, dbName);
#pragma warning restore CS0618
            var promotableLastCompareExchangeIndex = promotableDbStats.LastCompareExchangeIndex;
            if (leaderLastCompareExchangeIndex > promotableLastCompareExchangeIndex)
            {
                var msg = $"The database '{dbName}' on {promotable} not ready to be promoted, because not all of the compare exchanges have been sent yet." + Environment.NewLine +
                          $"Last Compare Exchange Raft Index: {promotableLastCompareExchangeIndex}" + Environment.NewLine +
                          $"Leader's Compare Exchange Raft Index: {leaderLastCompareExchangeIndex}";

                _logger.Log($"Node {promotable} hasn't been promoted because it's raft index isn't updated yet", state.ObserverIteration, database: dbName);

                if (topology.DemotionReasons.TryGetValue(promotable, out var demotionReason) == false ||
                    msg.Equals(demotionReason) == false)
                {
                    topology.DemotionReasons[promotable] = msg;
                    topology.PromotablesStatus[promotable] = DatabasePromotionStatus.RaftIndexNotUpToDate;
                    return (false, msg);
                }
                return (false, null);
            }

            var databaseEtag = -1L;
            if (state.HasActiveMigrations() == false)
            {
                // if resharding is active skip - index staleness will not prevent it from being promoted
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "This only a workaround until RavenDB-21327 will be fixed properly");
                databaseEtag = promotablePrevDbStats.LastEtag;
            }

            var indexesCaughtUp = CheckIndexProgress(
                databaseEtag,
                promotablePrevDbStats.LastIndexStats,
                promotableDbStats.LastIndexStats,
                mentorCurrDbStats.LastIndexStats,
                out var reason);

            if (indexesCaughtUp)
            {
                _logger.Log($"We try to promote the database '{dbName}' on {promotable} to be a full member", state.ObserverIteration, database: dbName);

                topology.PromotablesStatus.Remove(promotable);
                topology.DemotionReasons.Remove(promotable);

                return (true, $"Node {promotable} is up-to-date so promoting it to be member");
            }

            _logger.Log($"The database '{dbName}' on {promotable} is not ready to be promoted, because {reason}{Environment.NewLine}", state.ObserverIteration, database: dbName);

            if (topology.PromotablesStatus.TryGetValue(promotable, out var currentStatus) == false
                || currentStatus != DatabasePromotionStatus.IndexNotUpToDate)
            {
                var msg = $"Node {promotable} not ready to be a member, because the indexes are not up-to-date";
                topology.PromotablesStatus[promotable] = DatabasePromotionStatus.IndexNotUpToDate;
                topology.DemotionReasons[promotable] = msg;
                return (false, msg);
            }
            return (false, null);
        }

        protected virtual void RemoveOtherNodesIfNeeded(DatabaseObservationState state, ref List<DeleteDatabaseCommand> deletions)
        {
            var topology = state.DatabaseTopology;
            var dbName = state.Name;
            var clusterTopology = state.ClusterTopology;

            if (topology.Members.Count < topology.ReplicationFactor)
                return;

            if (topology.Promotables.Count == 0 &&
                topology.Rehabs.Count == 0)
                return;

            var nodesToDelete = new List<string>();
            var mentorChangeVector = new Dictionary<string, string>();

            foreach (var node in topology.Promotables.Concat(topology.Rehabs))
            {
                if (TryGetMentorNode(dbName, topology, clusterTopology, node, out var mentorNode) == false ||
                    state.Current.TryGetValue(mentorNode, out var mentorStats) == false ||
                    mentorStats.Report.TryGetValue(dbName, out var dbReport) == false)
                {
                    continue;
                }
                if (state.ReadDeletionInProgress()?.ContainsKey(node) == true)
                {
                    continue;
                }
                nodesToDelete.Add(node);
                mentorChangeVector.Add(node, dbReport.DatabaseChangeVector);
            }

            if (nodesToDelete.Count == 0)
                return;

            _logger.Log($"We reached the replication factor on database '{dbName}', so we try to remove redundant nodes from {string.Join(", ", nodesToDelete)}.", state.ObserverIteration, database: dbName);

            var deletionCmd = new DeleteDatabaseCommand(dbName, RaftIdGenerator.NewId())
            {
                ErrorOnDatabaseDoesNotExists = false,
                FromNodes = nodesToDelete.ToArray(),
                HardDelete = _hardDeleteOnReplacement,
                UpdateReplicationFactor = false,
            };

            if (deletions == null)
                deletions = new List<DeleteDatabaseCommand>();
            deletions.Add(deletionCmd);
        }

        private static List<string> GetPendingDeleteNodes(DatabaseObservationState state)
        {
            var alreadyInDeletionProgress = new List<string>();
            if (ShardHelper.TryGetShardNumberFromDatabaseName(state.Name, out var shardNumber))
            {
                foreach (var (tag, _) in state.RawDatabase.DeletionInProgress)
                {
                    if (tag.Contains($"${shardNumber}"))
                    {
                        alreadyInDeletionProgress.Add(tag.Replace($"${shardNumber}", ""));
                    }
                }

                return alreadyInDeletionProgress;
            }

            alreadyInDeletionProgress.AddRange(state.RawDatabase.DeletionInProgress?.Keys);
            return alreadyInDeletionProgress;
        }

        private enum DatabaseHealth
        {
            NotEnoughInfo,
            Bad,
            Good
        }

        private DatabaseHealth FailedDatabaseInstanceOrNode(
            string node,
            DatabaseObservationState state,
            out ClusterNodeStatusReport nodeStats)
        {
            var clusterTopology = state.ClusterTopology;
            var current = state.Current;
            var db = state.Name;

            var nodeHealth = CheckNodeHealth(node, clusterTopology, current, out nodeStats);
            if (nodeHealth != DatabaseHealth.Good)
                return nodeHealth;

            var currentNodeStats = current[node];

            if (currentNodeStats.LastGoodDatabaseStatus.TryGetValue(db, out var lastGoodTime) == false)
            {
                // here we have a problem, the databaseTopology says that the db needs to be in the node, but the node
                // doesn't know that the db is on it, that probably indicate some problem and we'll move it
                // to another node to resolve it.
                return DatabaseHealth.NotEnoughInfo;
            }
            if (lastGoodTime == default(DateTime) || lastGoodTime == DateTime.MinValue)
                return DatabaseHealth.NotEnoughInfo;

            return DateTime.UtcNow - lastGoodTime > _breakdownTimeout ? DatabaseHealth.Bad : DatabaseHealth.Good;
        }

        private DatabaseHealth CheckNodeHealth(string node, ClusterTopology clusterTopology, Dictionary<string, ClusterNodeStatusReport> current, out ClusterNodeStatusReport nodeStats)
        {
            if (clusterTopology.Contains(node) == false) // this node is no longer part of the *Cluster* databaseTopology and need to be replaced.
            {
                nodeStats = null;
                return DatabaseHealth.Bad;
            }

            var hasCurrent = current.TryGetValue(node, out nodeStats);

            // Wait until we have more info
            if (hasCurrent == false)
                return DatabaseHealth.NotEnoughInfo;

            // if server is down we should reassign
            if (DateTime.UtcNow - nodeStats.LastSuccessfulUpdateDateTime > _breakdownTimeout)
            {
                if (DateTime.UtcNow - _observerStartTime < _breakdownTimeout)
                    return DatabaseHealth.NotEnoughInfo;

                return DatabaseHealth.Bad;
            }

            return DatabaseHealth.Good;
        }

        private bool TryFindFitNode(string badNode, DatabaseObservationState state, DatabaseTopology topology, ClusterTopology clusterTopology, Dictionary<string, ClusterNodeStatusReport> current, string database, out string bestNode)
        {
            bestNode = null;
            var dbCount = int.MaxValue;

            var databaseNodes = topology.AllNodes.ToList();

            if (topology.Members.Count == 0) // no one can be used as mentor
                return false;

            foreach (var node in clusterTopology.AllNodes.Keys)
            {
                if (databaseNodes.Contains(node))
                    continue;

                if (FailedDatabaseInstanceOrNode(node, state, out _) == DatabaseHealth.Bad)
                    continue;

                if (current.TryGetValue(node, out var nodeReport) == false)
                {
                    if (bestNode == null)
                        bestNode = node;
                    continue;
                }

                if (dbCount > nodeReport.Report.Count)
                {
                    dbCount = nodeReport.Report.Count;
                    bestNode = node;
                }
            }

            if (bestNode == null)
            {
                _logger.Log($"The database '{database}' on {badNode} has not responded for a long time, but there is no free node to reassign it.", state.ObserverIteration, database: database);
                return false;
            }
            _logger.Log($"The database '{database}' on {badNode} has not responded for a long time, so we reassign it to {bestNode}.", state.ObserverIteration, database: database);

            return true;
        }

        private string FindMostUpToDateNode(List<string> nodes, string database, Dictionary<string, ClusterNodeStatusReport> current)
        {
            var updated = nodes[0];
            var highestChangeVectors = current[updated].Report[database].DatabaseChangeVector;
            var maxDocsCount = current[updated].Report[database].NumberOfDocuments;
            for (var index = 1; index < nodes.Count; index++)
            {
                var node = nodes[index];
                var report = current[node].Report[database];
                var cv = report.DatabaseChangeVector;
                var status = ChangeVectorUtils.GetConflictStatus(cv, highestChangeVectors);
                if (status == ConflictStatus.Update)
                {
                    highestChangeVectors = cv;
                }
                // In conflict we need to choose between 2 nodes that are not synced.
                // So we take the one with the most documents.
                if (status == ConflictStatus.Conflict)
                {
                    if (report.NumberOfDocuments > maxDocsCount)
                    {
                        highestChangeVectors = cv;
                        maxDocsCount = report.NumberOfDocuments;
                        updated = node;
                    }
                }
            }
            return updated;
        }

        private static bool CheckIndexProgress(
            long lastPrevEtag,
            Dictionary<string, DatabaseStatusReport.ObservedIndexStatus> previous,
            Dictionary<string, DatabaseStatusReport.ObservedIndexStatus> current,
            Dictionary<string, DatabaseStatusReport.ObservedIndexStatus> mentor,
            out string reason)
        {
            /*
            Here we are being a bit tricky. A database node is consider ready for promotion when
            it's replication is one cycle behind its mentor, but there are still indexes to consider.

            If we just replicate a whole bunch of stuff and indexes are catching up, we want to only
            promote when the indexes actually caught up. We do that by also requiring that all indexes
            will be either fully caught up (non stale) or that they are at most a single cycle behind.

            This is check by looking at the global etag from the previous round, and comparing it to the
            last etag that each index indexed in the current round. Note that technically, we need to compare
            on a per collection basis, but we can avoid it by noting that if the collection's last etag is
            not beyond the previous max etag, then the index will therefor not be non stale.

             */

            foreach (var mentorIndex in mentor)
            {
                // we go over all of the mentor indexes to validated that the promotable has them.
                // Since we don't save in the state machine the definition of side-by-side indexes, we will skip them, because
                // the promotable don't have them.

                if (mentorIndex.Value.IsSideBySide)
                    continue;

                if (mentorIndex.Value.State == IndexState.Idle)
                    continue;

                if (mentor.TryGetValue(Constants.Documents.Indexing.SideBySideIndexNamePrefix + mentorIndex.Key, out var mentorIndexStats) == false)
                {
                    mentorIndexStats = mentorIndex.Value;
                }

                if (previous.TryGetValue(mentorIndex.Key, out _) == false)
                {
                    reason = $"Index '{mentorIndex.Key}' is missing";
                    return false;
                }

                if (current.TryGetValue(mentorIndex.Key, out var currentIndexStats) == false)
                {
                    reason = $"Index '{mentorIndex.Key}' is missing";
                    return false;
                }

                if (currentIndexStats.State == IndexState.Error)
                {
                    if (mentorIndexStats.State == IndexState.Error)
                        continue;
                    reason = $"Index '{mentorIndex.Key}' is in state '{currentIndexStats.State}'";
                    return false;
                }

                if (currentIndexStats.IsStale == false)
                    continue;

                if (mentorIndexStats.LastIndexedEtag == (long)Index.IndexProgressStatus.Faulty)
                {
                    continue; // skip the check for faulty indexes
                }

                if (currentIndexStats.State == IndexState.Disabled)
                    continue;

                var lastIndexEtag = currentIndexStats.LastIndexedEtag;
                if (lastPrevEtag > lastIndexEtag)
                {
                    reason = $"Index '{mentorIndex.Key}' is in state '{currentIndexStats.State}' and not up-to-date (prev database etag: {lastPrevEtag:#,#;;0}, current indexed etag: {lastIndexEtag:#,#;;0}).";
                    return false;
                }
            }

            reason = null;
            return true;
        }

        private const string ThingsToCheck = "Things you may check: verify node is working, check for ports being blocked by firewall or similar software.";

        private void RaiseNoLivingNodesAlert(string alertMsg, string dbName, long iteration)
        {
            var alert = AlertRaised.Create(
                dbName,
                $"Could not reach any node of '{dbName}' database",
                $"{alertMsg}. {ThingsToCheck}",
                AlertType.DatabaseTopologyWarning,
                NotificationSeverity.Warning
            );

            _server.NotificationCenter.Add(alert, updateExisting: false);
            _logger.Log(alertMsg, iteration, database: dbName);
        }

        private void RaiseNodeNotFoundAlert(string alertMsg, string node, long iteration)
        {
            var alert = AlertRaised.Create(
                null,
                $"Node {node} not found.",
                $"{alertMsg}",
                AlertType.DatabaseTopologyWarning,
                NotificationSeverity.Warning
            );

            _server.NotificationCenter.Add(alert, updateExisting: false);
            _logger.Log(alertMsg, iteration);
        }
    }
}
