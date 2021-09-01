using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding.Documents
{
    public class ShardedSubscriptionContext : SubscriptionStorageBase, IDisposable /*, ILowMemoryHandler*/
    {
        private readonly ShardedContext _shardedContext;
        private readonly Logger _logger;
        private readonly ServerStore _serverStore;

        public ShardedSubscriptionContext(ShardedContext shardedContext, ServerStore serverStore) : base(serverStore)
        {
            _shardedContext = shardedContext;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<ShardedSubscriptionContext>(_shardedContext.DatabaseName);
        }

        public SubscriptionConnectionState OpenSubscription(SubscriptionConnectionBase connection)
        {
            var subscriptionState = _subscriptionConnectionStates.GetOrAdd(connection.SubscriptionId,
                subscriptionId => new SubscriptionConnectionState(subscriptionId, connection.Options.SubscriptionName, this));

            return subscriptionState;
        }

        public async Task<bool> DropSubscriptionConnection(long subscriptionId, SubscriptionException ex)
        {
            if (_subscriptionConnectionStates.TryGetValue(subscriptionId, out SubscriptionConnectionState subscriptionConnectionState) == false)
                return false;

            // drop on shards
            var disposables = new List<IDisposable>();
            try
            {
                var tasks = new List<Task>();
                var cmds = new List<DropSubscriptionConnectionCommand>();
                foreach (var re in _shardedContext.RequestExecutors)
                {
                    disposables.Add(_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx));
                    var cmd = new DropSubscriptionConnectionCommand(subscriptionConnectionState.SubscriptionName);
                    cmds.Add(cmd);
                    tasks.Add(re.ExecuteAsync(cmd, ctx));
                }

                await Task.WhenAll(tasks);
            }
            finally
            {
                disposables.ForEach(x => x.Dispose());
            }

            var subscriptionConnection = subscriptionConnectionState.Connection;

            if (subscriptionConnection != null)
            {
                // wait for propogate from the server workers to sharded connection
                if (subscriptionConnection.CancellationTokenSource.Token.WaitHandle.WaitOne(15000) == false)
                {
                    try
                    {
                        subscriptionConnection.CancellationTokenSource.Cancel();
                    }
                    catch
                    {
                        // ignored
                    }
                }

                subscriptionConnectionState.RegisterRejectedConnection(subscriptionConnection, ex);
                subscriptionConnection.ConnectionException = ex;
            }

            if (_logger.IsInfoEnabled)
                _logger.Info($"Sharded subscription with id '{subscriptionId}' and name '{subscriptionConnectionState.SubscriptionName}' connection was dropped. Reason: {ex.Message}");

            return true;
        }

        public async Task<SubscriptionState> AssertSubscriptionConnectionDetails(long id, string name)
        {
            await _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, id);

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            using (var record = _serverStore.Cluster.ReadRawDatabaseRecord(serverStoreContext, _shardedContext.DatabaseName))
            {
                if (record == null)
                    throw new DatabaseDoesNotExistException($"Stopping sharded subscription '{name}' on node {_serverStore.NodeTag}, because database '{_shardedContext.DatabaseName}' doesn't exists.");

                if (record.DeletionInProgress.Count > 0 && record.DeletionInProgress.Any(x => x.Key.StartsWith(_serverStore.NodeTag)))
                    throw new DatabaseDoesNotExistException($"Stopping sharded subscription '{name}' on node {_serverStore.NodeTag}, because database '{_shardedContext.DatabaseName}' is being deleted.");

                var subscription = GetShardedSubscriptionFromServerStore(_serverStore, serverStoreContext, _shardedContext, name);

                if (subscription.Disabled)
                    throw new SubscriptionClosedException($"The subscription with id '{id}' and name '{name}' is disabled and cannot be used until enabled");

                return subscription;
            }
        }

        public SubscriptionGeneralDataAndStats GetRunningSubscription(TransactionOperationContext context, long? id, string name, bool history)
        {
            SubscriptionGeneralDataAndStats subscription;
            if (string.IsNullOrEmpty(name) == false)
            {
                subscription = GetShardedSubscriptionFromServerStore(_serverStore, context, _shardedContext, name);
            }
            else if (id.HasValue)
            {
                name = GetSubscriptionNameById(context, _shardedContext.GetShardedDatabaseName(), id.Value);
                subscription = GetShardedSubscriptionFromServerStore(_serverStore, context, _shardedContext, name);
            }
            else
            {
                throw new ArgumentNullException("Must receive either subscription id or subscription name in order to provide subscription data");
            }

            if (_subscriptionConnectionStates.TryGetValue(subscription.SubscriptionId, out SubscriptionConnectionState subscriptionConnectionState) == false)
                return null;

            if (subscriptionConnectionState.Connection == null)
                return null;

            GetRunningSubscriptionInternal(history, subscription, subscriptionConnectionState);
            return subscription;
        }

        public static SubscriptionGeneralDataAndStats GetShardedSubscriptionFromServerStore(ServerStore serverStore, TransactionOperationContext context, ShardedContext shardedContext, string name)
        {
            using BlittableJsonReaderObject subscriptionBlittable = serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(shardedContext.DatabaseName, name));

            if (subscriptionBlittable == null)
                ThrowNotFoundException(name);

            var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);
            var subscriptionJsonValue = new SubscriptionGeneralDataAndStats(subscriptionState);
            return subscriptionJsonValue;
        }

        public SubscriptionConnectionState GetSubscriptionConnection(TransactionOperationContext context, string subscriptionName)
        {
            using var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(_shardedContext.DatabaseName, subscriptionName));
            if (subscriptionBlittable == null)
                return null;

            var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);

            if (_subscriptionConnectionStates.TryGetValue(subscriptionState.SubscriptionId, out SubscriptionConnectionState subscriptionConnection) == false)
            {
                return null;
            }

            return subscriptionConnection;
        }

        private static void ThrowNotFoundException(string name)
        {
            throw new SubscriptionDoesNotExistException($"Subscription with name '{name}' was not found in server store");
        }

        public List<string> GetResponsibleNodes(TransactionOperationContext context, string name)
        {
            var nodes = new List<string>();
            var subscription = GetSubscriptionFromServerStore(_serverStore, context, _shardedContext.DatabaseName, name);

            foreach (var topology in _shardedContext.ShardsTopology)
            {
                var node = topology.WhoseTaskIsIt(_serverStore.Engine.CurrentState, subscription, null);
                if (node == null)
                    continue;

                nodes.Add(node);
            }

            return nodes;
        }

        public void Dispose()
        {
            foreach (var kvp in _subscriptionConnectionStates)
            {
                kvp.Value.Dispose();
            }
        }
    }
}
