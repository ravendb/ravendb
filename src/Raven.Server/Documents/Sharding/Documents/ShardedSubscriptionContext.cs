using System;
using System.Collections.Concurrent;
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
    public class ShardedSubscriptionContext
    {
        private readonly ShardedContext _shardedContext;
        private readonly ConcurrentDictionary<long, SubscriptionConnectionState> _subscriptionConnectionStates = new ConcurrentDictionary<long, SubscriptionConnectionState>();
        private readonly Logger _logger;
        private readonly ServerStore _serverStore;

        public ShardedSubscriptionContext(ShardedContext shardedContext, ServerStore serverStore)
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
                {
                    throw new DatabaseDoesNotExistException($"Stopping sharded subscription '{name}' on node {_serverStore.NodeTag}, because database '{_shardedContext.DatabaseName}' is being deleted.");
                }

                var subscription = GetShardedSubscriptionFromServerStore(_serverStore, serverStoreContext, _shardedContext, name);

                return subscription;
            }
        }

        public SubscriptionStorage.SubscriptionGeneralDataAndStats GetRunningSubscription(TransactionOperationContext context, long? id, string name, bool history)
        {
            SubscriptionStorage.SubscriptionGeneralDataAndStats subscription;
            if (string.IsNullOrEmpty(name) == false)
            {
                subscription = GetShardedSubscriptionFromServerStore(_serverStore, context, _shardedContext, name);
            }
            else if (id.HasValue)
            {
                name = SubscriptionStorage.GetSubscriptionNameById(context, _shardedContext.GetShardedDatabaseName(), id.Value);
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

            SubscriptionStorage.GetRunningSubscriptionInternal(history, subscription, subscriptionConnectionState);
            return subscription;
        }

        public static SubscriptionStorage.SubscriptionGeneralDataAndStats GetShardedSubscriptionFromServerStore(ServerStore serverStore, TransactionOperationContext context, ShardedContext shardedContext, string name)
        {
            var list = new List<BlittableJsonReaderObject>();
            for (int i = 0; i < shardedContext.Count; i++)
            {
                BlittableJsonReaderObject subscriptionBlittable = serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(shardedContext.GetShardedDatabaseName(i), name));
                list.Add(subscriptionBlittable);
            }

            if (list.Count == 0)
                ThrowNotFoundException(name);

            var first = list[0];
            var max = list[0];
            foreach (var bjro in list)
            {
                if (bjro == null)
                    ThrowNotFoundException(name);

                if (AssertEqualSubscription(bjro, first) == false)
                    ThrowNotEqualException(name);

                max = GetMaxSubscription(bjro, first);
            }

            //TODO: egor, return latest ? by client connect time ? by last batch ack? by cv ??
            var subscriptionState = JsonDeserializationClient.SubscriptionState(max);
            var subscriptionJsonValue = new SubscriptionStorage.SubscriptionGeneralDataAndStats(subscriptionState);
            return subscriptionJsonValue;
        }

        public SubscriptionConnectionState GetSubscriptionConnection(TransactionOperationContext context, string subscriptionName)
        {
            var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(_shardedContext.GetShardedDatabaseName(), subscriptionName));
            if (subscriptionBlittable == null)
                return null;

            var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);

            if (_subscriptionConnectionStates.TryGetValue(subscriptionState.SubscriptionId, out SubscriptionConnectionState subscriptionConnection) == false)
                return null;

            return subscriptionConnection;
        }

        private static void ThrowNotFoundException(string name)
        {
            throw new SubscriptionDoesNotExistException($"Subscription with name '{name}' was not found in server store");
        }
        private static void ThrowNotEqualException(string name)
        {
            throw new SubscriptionDoesNotExistException($"Subscription with name '{name}' was not equal in server store");
        }

        private static bool AssertEqualSubscription(BlittableJsonReaderObject b1, BlittableJsonReaderObject b2)
        {
            if (b1.TryGet(nameof(SubscriptionState.SubscriptionId), out long id1) == false ||
                b2.TryGet(nameof(SubscriptionState.SubscriptionId), out long id2) == false || id1 != id2)
                return false;

            if (b1.TryGet(nameof(SubscriptionState.SubscriptionName), out string name1) == false || string.IsNullOrEmpty(name1) ||
                b2.TryGet(nameof(SubscriptionState.SubscriptionName), out string name2) == false || string.IsNullOrEmpty(name2) || name1 != name2)
                return false;

            if (b1.TryGet(nameof(SubscriptionState.Query), out string query1) == false || query1 == null ||
                b2.TryGet(nameof(SubscriptionState.Query), out string query2) == false || query2 == null || query1 != query2)
                return false;

            return true;
        }

        private static BlittableJsonReaderObject GetMaxSubscription(BlittableJsonReaderObject b1, BlittableJsonReaderObject b2)
        {
            b1.TryGet(nameof(SubscriptionState.LastBatchAckTime), out DateTime? lastBatchAckTime1);
            if (lastBatchAckTime1 == null)
                return b2;
            b2.TryGet(nameof(SubscriptionState.LastBatchAckTime), out DateTime? lastBatchAckTime2);
            if (lastBatchAckTime2 == null)
                return b1;

            return lastBatchAckTime1 > lastBatchAckTime2 ? b1 : b2;
        }
    }
}
