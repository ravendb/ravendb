using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Subscriptions
{
    public class ShardedSubscriptionWorker : AbstractSubscriptionWorker<ShardedSubscriptionBatch, BlittableJsonReaderObject>
    {
        private readonly int _shardNumber;
        private readonly RequestExecutor _shardRequestExecutor;
        private readonly SubscriptionConnectionsStateOrchestrator _state;
        
        public ShardedSubscriptionWorker(SubscriptionWorkerOptions options, string dbName, RequestExecutor re, SubscriptionConnectionsStateOrchestrator state) : base(options, dbName)
        {
            _shardNumber = ShardHelper.GetShardNumber(dbName);
            _shardRequestExecutor = re;
            _state = state;

            AfterAcknowledgment += batch =>
            {
                batch.ConfirmFromShardSubscriptionConnectionTcs.TrySetResult();
                return Task.CompletedTask;
            };
        }

        protected override RequestExecutor GetRequestExecutor() => _shardRequestExecutor;

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
        protected override ShardedSubscriptionBatch CreateEmptyBatch() => new ShardedSubscriptionBatch(_subscriptionLocalRequestExecutor, _dbName, _logger);

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

        protected override (bool ShouldTryToReconnect, ServerNode NodeRedirectTo) CheckIfShouldReconnectWorker(Exception ex, CancellationTokenSource processingCts,
            Action assertLastConnectionFailure, Action<Exception> onUnexpectedSubscriptionError, bool throwOnRedirectNodeNotFound = true)
        {
            // always try to reconnect until the task is canceled or assertLastConnectionFailure will throw when 'MaxErroneousPeriod' will elapse
            try
            {
                assertLastConnectionFailure.Invoke();
                var r = base.CheckIfShouldReconnectWorker(ex, default, assertLastConnectionFailure, onUnexpectedSubscriptionError, throwOnRedirectNodeNotFound: false);

                if (_state.CancellationTokenSource.IsCancellationRequested)
                {
                    processingCts.Cancel();
                    return (ShouldTryToReconnect: false, NodeRedirectTo: null);
                }

                return (ShouldTryToReconnect: true, r.NodeRedirectTo);
            }
            // here we need to cancel all other sharded works as well
            catch (SubscriptionException se)
            {
                _state.DropSubscription(se);
                throw;
            }
            catch (Exception e)
            {
                _state.DropSubscription(new SubscriptionClosedException($"Stopping sharded subscription '{_options.SubscriptionName}' with id '{_state.SubscriptionId}'", canReconnect: true, e));
                throw;
            }
        }
    }
}
