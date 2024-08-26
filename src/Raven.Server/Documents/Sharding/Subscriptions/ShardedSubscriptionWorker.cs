using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Logging;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding.Subscriptions
{
    public sealed class ShardedSubscriptionWorker : AbstractSubscriptionWorker<ShardedSubscriptionBatch, BlittableJsonReaderObject>
    {
        private readonly int _shardNumber;
        private RequestExecutor _shardRequestExecutor;
        private readonly SubscriptionConnectionsStateOrchestrator _state;
        private bool _closedDueNoDocsLeft;
        private readonly ShardedDatabaseContext _databaseContext;
        public Task SubscriptionTask => _subscriptionTask;

        public ShardedSubscriptionWorker(SubscriptionWorkerOptions options, string dbName, RequestExecutor re, SubscriptionConnectionsStateOrchestrator state) : base(options, dbName, RavenLogManager.Instance.GetLoggerForDatabase<ShardedSubscriptionWorker>(dbName))
        {
            _shardNumber = ShardHelper.GetShardNumberFromDatabaseName(dbName);
            _shardRequestExecutor = re;
            _state = state;
            _databaseContext = state._databaseContext;
            AfterAcknowledgment += batch =>
            {
                batch.ConfirmFromShardSubscriptionConnectionTcs.TrySetResult();
                return Task.CompletedTask;
            };
        }

        internal override async Task<BatchFromServer> PrepareBatchAsync(JsonContextPool contextPool, Stream tcpStreamCopy, JsonOperationContext.MemoryBuffer buffer,
            ShardedSubscriptionBatch batch, Task notifiedSubscriber)
        {
            try
            {
                return await base.PrepareBatchAsync(contextPool, tcpStreamCopy, buffer, batch, notifiedSubscriber);
            }
            catch (Exception e)
            {
                batch.SetException(e);
                throw;
            }
        }

        protected override async Task SendAckAsync(ShardedSubscriptionBatch batch, Stream stream, JsonOperationContext context, CancellationToken token)
        {
            try
            {
                await SendAckInternalAsync(batch, stream, context, token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                batch.SetException(e);
                throw;
            }
        }

        protected override Task TrySetRedirectNode(RequestExecutor reqEx, IReadOnlyList<ServerNode> curTopology)
        {
            if (_databaseContext.ShardsTopology.TryGetValue(_shardNumber, out var topology))
            {
                var node = topology.WhoseTaskIsIt(RachisState.Follower, _state.SubscriptionState, null);
                if (node == null)
                    return Task.CompletedTask;

                _redirectNode = _shardRequestExecutor.TopologyNodes.FirstOrDefault(x => x.ClusterTag == node);
            }

            return Task.CompletedTask;
        }

        protected override RequestExecutor GetRequestExecutor()
        {
            if (_shardRequestExecutor == null || _shardRequestExecutor.Disposed)
            {
                _shardRequestExecutor = _databaseContext.ShardExecutor.GetRequestExecutorAt(_shardNumber);
            }
            return _shardRequestExecutor;
        }

        protected override void SetLocalRequestExecutor(string url, X509Certificate2 cert)
        {
            using (var old = _subscriptionLocalRequestExecutor)
            {
                _subscriptionLocalRequestExecutor = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, _dbName, cert, DocumentConventions.Default);
            }
        }

        /*
         *** ShardedSubscriptionWorker batch handling flow:
         * 1. reads batch from shard
         * 2. publish the batch
         * 3. Wait for ShardedSubscriptionConnection to redirect the batch to client (and receive ACK request for it)
         * 4. Send ACK request to shard and wait for CONFIRM from shard
         * 5. Set TCS so ShardedSubscriptionConnection will send CONFIRM to the client
         * 6. continue processing Subscription
         */
        protected override ShardedSubscriptionBatch CreateEmptyBatch() => new ShardedSubscriptionBatch(_subscriptionLocalRequestExecutor, _dbName, _logger, _databaseContext);

        public async Task TryPublishBatchAsync(ShardedSubscriptionBatch batch)
        {
            try
            {
                _processingCts.Token.ThrowIfCancellationRequested();
                _state.Batches.Add(batch);

                _state.NotifyHasMoreDocs();
                await using (_processingCts.Token.Register(batch.SetCancel))
                {
                    // wait for ShardedSubscriptionConnection to redirect the batch to client worker
                    await batch.SendBatchToClientTcs.Task;
                }
            }
            catch (Exception e)
            {
                batch.SetException(e);
                throw;
            }
        }

        protected override (bool ShouldTryToReconnect, ServerNode NodeRedirectTo) CheckIfShouldReconnectWorker(Exception ex, Action assertLastConnectionFailure, Action<Exception> onUnexpectedSubscriptionError, bool throwOnRedirectNodeNotFound = true)
        {
            // always try to reconnect until the task is canceled or assertLastConnectionFailure will throw when 'MaxErroneousPeriod' will elapse
            try
            {
                if (_state.CancellationTokenSource.IsCancellationRequested)
                {
                    _processingCts.Cancel();
                    return (ShouldTryToReconnect: false, NodeRedirectTo: null);
                }

                // assertLastConnectionFailure will be null in case of AggregateException
                assertLastConnectionFailure?.Invoke();

                var r = base.CheckIfShouldReconnectWorker(ex, assertLastConnectionFailure: null, onUnexpectedSubscriptionError, throwOnRedirectNodeNotFound: false);

                if (_closedDueNoDocsLeft)
                    return (ShouldTryToReconnect: false, NodeRedirectTo: null);

                return (r.ShouldTryToReconnect, r.NodeRedirectTo);
            }
            // here we need to cancel all other sharded works as well
            catch (SubscriptionException se)
            {
                _state.DropSubscription(se);
                throw;
            }
            catch (Exception e)
            {
                _state.DropSubscription(new SubscriptionClosedException($"Stopping sharded subscription '{_options.SubscriptionName}' with id '{_state.SubscriptionId}'", canReconnect: true, noDocsLeft: false, e));
                throw;
            }
        }

        protected override (bool ShouldTryToReconnect, ServerNode NodeRedirectTo) HandleSubscriptionClosedException(SubscriptionClosedException sce)
        {
            if (sce.NoDocsLeft)
            {
                Interlocked.Increment(ref _state.ClosedDueToNoDocs);
                _closedDueNoDocsLeft = true;
                return (false, null);
            }

            if (sce.CanReconnect)
                return (true, _redirectNode);

            return (false, null);
        }

        protected override (bool ShouldTryToReconnect, ServerNode NodeRedirectTo) HandleShouldNotTryToReconnect()
        {
            return (true, _redirectNode);
        }

        protected override (bool ShouldTryToReconnect, ServerNode NodeRedirectTo) HandleAggregateException()
        {
            return (true, _redirectNode);
        }

        protected override (bool ShouldTryToReconnect, ServerNode NodeRedirectTo) HandleSubscriptionChangeVectorUpdateConcurrencyException(SubscriptionChangeVectorUpdateConcurrencyException subscriptionChangeVectorUpdateConcurrencyException)
        {
            // the orchestrator will reconnect (and restart the sharded workers) since the subscription was changed
            _state.DropSubscription(subscriptionChangeVectorUpdateConcurrencyException);

            return (false, null);
        }

        protected override void HandleSubscriberError(Exception ex)
        {
            // for sharded worker we throw the real exception
            throw ex;
        }

        protected override TimeSpan GetTimeToWaitBeforeConnectionRetry()
        {
            if (_redirectNode == null)
            {
                // don't spam the subscription connection if there is real error or unstable cluster
                return TimeSpan.FromSeconds(3);
            }

            // we got SubscriptionDoesNotBelongToNodeException with an AppropriateNode set
            return _options.TimeToWaitBeforeConnectionRetry;
        }

        protected override async Task TrySetRedirectNodeOnConnectToServerAsync()
        {
            if (_databaseContext.ShardsTopology.TryGetValue(_shardNumber, out var topology))
            {
                var node = topology.WhoseTaskIsIt(RachisState.Follower, _state.SubscriptionState, null);
                if (node == null)
                    return;

                if (_shardRequestExecutor == null || _shardRequestExecutor.Disposed)
                {
                    GetRequestExecutor();
                    _redirectNode = await TryGetRequestedNode(_shardRequestExecutor, node) ?? _redirectNode;
                    return;
                }

                if (_shardRequestExecutor.TopologyNodes == null)
                {
                    _redirectNode = await TryGetRequestedNode(_shardRequestExecutor, node) ?? _redirectNode;
                    return;
                }

                _redirectNode = _shardRequestExecutor.TopologyNodes.FirstOrDefault(x => x.ClusterTag == node);
            }
        }

        private async Task<ServerNode> TryGetRequestedNode(RequestExecutor re, string node)
        {
            try
            {
                return (await re.GetRequestedNode(node)).Node;
            }
            catch
            {
                return null;
            }
        }
    }
}
