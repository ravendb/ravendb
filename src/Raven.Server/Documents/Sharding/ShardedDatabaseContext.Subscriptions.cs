using System;
using System.Collections.Generic;
using System.Net;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;
using static Raven.Server.Documents.Subscriptions.SubscriptionStorage;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public ShardedSubscriptionsStorage SubscriptionsStorage;

    public sealed class ShardedSubscriptionsStorage : AbstractSubscriptionStorage<SubscriptionConnectionsStateOrchestrator>
    {
        private readonly ShardedDatabaseContext _context;

        public ShardedSubscriptionsStorage(ShardedDatabaseContext context, ServerStore serverStore) : base(serverStore, context.Configuration.Subscriptions.MaxNumberOfConcurrentConnections)
        {
            _context = context;
            _databaseName = _context.DatabaseName;
            _logger = LoggingSource.Instance.GetLogger<ShardedSubscriptionsStorage>(_databaseName);
        }

        protected override void DropSubscriptionConnections(SubscriptionConnectionsStateOrchestrator state, SubscriptionException ex)
        {
            foreach (var subscriptionConnection in state.GetConnections())
            {
                state.DropSingleConnection(subscriptionConnection, ex);
            }
        }

        protected override void SetConnectionException(SubscriptionConnectionsStateOrchestrator state, SubscriptionException ex)
        {
            foreach (var connection in state.GetConnections())
            {
                // this is just to set appropriate exception, the connections will be dropped on state dispose
                connection.ConnectionException = ex;
            }
        }

        protected override string GetNodeFromState(SubscriptionState taskStatus) => taskStatus.NodeTag;

        protected override DatabaseTopology GetTopology(ClusterOperationContext context) => _serverStore.Cluster.ReadShardingConfiguration(context, _databaseName).Orchestrator.Topology;

        protected override bool SubscriptionChangeVectorHasChanges(SubscriptionConnectionsStateOrchestrator state, SubscriptionState taskStatus)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Normal, "RavenDB-18223 Add ability to set CV by admin in sharded subscription.");
            return false;
        }

        public void Update()
        {
            HandleDatabaseRecordChange();
        }

        public override bool DropSingleSubscriptionConnection(long subscriptionId, string workerId, SubscriptionException ex)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Normal, "RavenDB-18568: need to handle workerId for concurrent subscription");
            // for sharded database there is no concurrent subscription
            return DropSubscriptionConnections(subscriptionId, ex);
        }

        public override bool DisableSubscriptionTasks => false;

        public override ArchivedDataProcessingBehavior GetDefaultArchivedDataProcessingBehavior()
        {
            return _context.Configuration.Subscriptions.ArchivedDataProcessingBehavior;
        }

        public override ShardedSubscriptionData GetSubscriptionWithDataByNameFromServerStore(ClusterOperationContext context, string name, bool history, bool running)
        {
            var state = GetSubscriptionByName(context, name);
            var shardedSubscriptionData = new ShardedSubscriptionData(new SubscriptionGeneralDataAndStats(state));
            var subscription = GetSubscriptionInternal(shardedSubscriptionData, history, running);

            return subscription;
        }

        public override ShardedSubscriptionData GetSubscriptionWithDataByIdFromServerStore(ClusterOperationContext context, long id, bool history, bool running)
        {
            var state = GetSubscriptionById(context, id);
            var shardedSubscriptionData = new ShardedSubscriptionData(new SubscriptionGeneralDataAndStats(state));
            var subscription = GetSubscriptionInternal(shardedSubscriptionData, history, running);

            return subscription;
        }

        public override IEnumerable<ShardedSubscriptionData> GetAllSubscriptions(ClusterOperationContext context, bool history, int start, int take)
        {
            foreach (var task in GetAllSubscriptionsFromServerStore(context, start, take))
            {
                var shardedSubscriptionData = GetSubscriptionInternal(task, history, running: false);

                yield return shardedSubscriptionData;
            }
        }

        public IEnumerable<ShardedSubscriptionData> GetAllRunningSubscriptions(ClusterOperationContext context, bool history, int start, int take)
        {
            foreach (var (state, subscriptionConnectionsState) in GetAllRunningSubscriptionsInternal(context, history, start, take))
            {
                var shardedSubscriptionData = PopulateSubscriptionData(state, history, subscriptionConnectionsState);

                yield return shardedSubscriptionData;
            }
        }

        private ShardedSubscriptionData GetSubscriptionInternal(SubscriptionState state, bool history, bool running)
        {
            if (GetSubscriptionConnectionsStateAndCheckRunningIfNeeded(state, running, out SubscriptionConnectionsStateOrchestrator orchestratorSubscription) == null)
            {
                // not mine running subscription
                return null;
            }

            return PopulateSubscriptionData(state, history, orchestratorSubscription);
        }

        private static ShardedSubscriptionData PopulateSubscriptionData(SubscriptionState state, bool history, SubscriptionConnectionsStateOrchestrator orchestratorSubscription)
        {
            var shardedSubscriptionData = new ShardedSubscriptionData(new SubscriptionGeneralDataAndStats(state));
            if (orchestratorSubscription == null)
            {
                // not mine subscription
                return shardedSubscriptionData;
            }

            shardedSubscriptionData.ShardedWorkers = orchestratorSubscription.ShardWorkers;
            shardedSubscriptionData.Connections = orchestratorSubscription.GetConnections();

            if (history)
            {
                SetSubscriptionHistory(orchestratorSubscription, shardedSubscriptionData);
                shardedSubscriptionData.RecentShardedWorkers = orchestratorSubscription.RecentShardedWorkers;
            }

            return shardedSubscriptionData;
        }

        public sealed class ShardedSubscriptionData : SubscriptionDataBase<OrchestratedSubscriptionConnection>
        {
            public IDictionary<string, ShardedSubscriptionWorker> ShardedWorkers;
            public IEnumerable<ShardedSubscriptionWorkerInfo> RecentShardedWorkers;

            public ShardedSubscriptionData(SubscriptionGeneralDataAndStats stateWithData) : base(stateWithData)
            {
            }
        }

        public class ShardedSubscriptionWorkerInfo : IDynamicJson
        {
            public string Shard { get; set; }
            public string WorkerId { get; set; }
            public string RemoteIp { get; set; }
            public string CurrentNodeTag { get; set; }
            public DateTime? DisposeTimeUtc { get; set; }
            public DateTime? LastConnectionFailure { get; set; }
            public bool IsCanceled { get; set; }
            public bool IsFaulted { get; set; }
            public string Exception { get; set; }

            public ShardedSubscriptionWorkerInfo()
            {
            }

            public static ShardedSubscriptionWorkerInfo Create(string shard, ShardedSubscriptionWorker w)
            {
                string address = null;
                try
                {
                    var e = (IPEndPoint)w._tcpClient?.Client?.RemoteEndPoint;
                    if (e != null)
                    {
                        address = $"{e.Address}:{e.Port}";
                    }
                }
                catch
                {
                    // might be disposed
                }

                return new ShardedSubscriptionWorkerInfo()
                {
                    Shard = shard,
                    DisposeTimeUtc = DateTime.UtcNow,
                    Exception = w.SubscriptionTask.Exception?.ToString(),
                    IsCanceled = w.SubscriptionTask.IsCanceled,
                    IsFaulted = w.SubscriptionTask.IsFaulted,
                    CurrentNodeTag = w.CurrentNodeTag,
                    WorkerId = w.WorkerId,
                    LastConnectionFailure = w._lastConnectionFailure,
                    RemoteIp = address
                };
            }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Shard)] = Shard,
                    [nameof(WorkerId)] = WorkerId,
                    [nameof(RemoteIp)] = RemoteIp,
                    [nameof(CurrentNodeTag)] = CurrentNodeTag,
                    [nameof(DisposeTimeUtc)] = DisposeTimeUtc,
                    [nameof(LastConnectionFailure)] = LastConnectionFailure,
                    [nameof(IsCanceled)] = IsCanceled,
                    [nameof(IsFaulted)] = IsFaulted,
                    [nameof(Exception)] = Exception,
                };
            }
        }
    }
}
