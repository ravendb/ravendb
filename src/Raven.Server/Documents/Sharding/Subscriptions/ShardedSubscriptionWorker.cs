using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
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

        internal override RequestExecutor GetRequestExecutor() => _shardRequestExecutor;

        internal override void SetLocalRequestExecutor(string url, X509Certificate2 cert)
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

                // Set the start handling worker MRE on ShardedSubscriptionConnection
                _state.NotifyHasMoreDocs();
                await using (_processingCts.Token.Register(() =>
                             {
                                 batch.SendBatchToClientTcs.TrySetCanceled();
                                 batch.ConfirmFromShardSubscriptionConnectionTcs.TrySetCanceled();
                             }))
                {
                    await batch.SendBatchToClientTcs.Task;
                }
            }
            catch (Exception e)
            {
                batch.SendBatchToClientTcs.TrySetException(e);
                batch.ConfirmFromShardSubscriptionConnectionTcs.TrySetException(e);
                throw;
            }
            // wait for ShardedSubscriptionConnection to redirect the batch to client worker
        }
    }
}
