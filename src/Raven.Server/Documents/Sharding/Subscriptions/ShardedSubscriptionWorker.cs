using System;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Subscriptions
{
    public class ShardedSubscriptionWorker : AbstractSubscriptionWorker<dynamic>
    {
        private readonly RequestExecutor _shardRequestExecutor;
        private readonly SubscriptionConnectionsStateOrchestrator _state;
        
        public ShardedSubscriptionWorker(SubscriptionWorkerOptions options, string dbName, RequestExecutor re, SubscriptionConnectionsStateOrchestrator state) : base(options, dbName)
        {
            _shardRequestExecutor = re;
            _state = state;
        }

        internal override RequestExecutor GetRequestExecutor()
        {
            return _shardRequestExecutor;
        }

        internal override void SetLocalRequestExecutor(string url, X509Certificate2 cert)
        {
            using (var old = _subscriptionLocalRequestExecutor)
            {
                _subscriptionLocalRequestExecutor = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, _dbName, cert, DocumentConventions.Default);
            }
        }

        public class PublishedShardBatch
        {
            internal BatchFromServer _batchFromServer;
            public TaskCompletionSource SendBatchToClientTcs;
            public TaskCompletionSource ConfirmFromShardSubscriptionConnectionTcs;
            public string LastSentChangeVectorInBatch;
        }

        public PublishedShardBatch PublishedShardBatchItem;

        /*
         *** ShardedSubscriptionWorker batch handling flow:
         * 1. reads batch from shard
         * 2. publish the batch
         * 3. Wait for ShardedSubscriptionConnection to redirect the batch to client (and receive ACK request for it)
         * 4. Send ACK request to shard and wait for CONFIRM from shard
         * 5. Set TCS so ShardedSubscriptionConnection will send CONFIRM to the client
         * 6. continue processing Subscription
         */
        internal override async Task ProcessSubscriptionInternalAsync(JsonContextPool contextPool, Stream tcpStreamCopy, JsonOperationContext.MemoryBuffer buffer, JsonOperationContext context)
        {
            while (_processingCts.IsCancellationRequested == false)
            {
                BatchFromServer incomingBatch = await ReadSingleSubscriptionBatchFromServerAsync(contextPool, tcpStreamCopy, buffer, batch: null).ConfigureAwait(false);
                _processingCts.Token.ThrowIfCancellationRequested();

                try
                {
                    await TryPublishBatch(incomingBatch);

                    // send ack to SubscriptionConnection (to shard)
                    await SendAckAsync(PublishedShardBatchItem.LastSentChangeVectorInBatch, tcpStreamCopy, context, _processingCts.Token).ConfigureAwait(false);
                    // hard coded to get confirm of applying acknowledge command
                    SubscriptionConnectionServerMessage receivedMessage = await ReadNextObjectAsync(context, tcpStreamCopy, buffer).ConfigureAwait(false);
                    _processingCts.Token.ThrowIfCancellationRequested();
                    if (receivedMessage == null || receivedMessage.Type != SubscriptionConnectionServerMessage.MessageType.Confirm)
                    {
                        string name;
                        string exception;
                        if (receivedMessage == null)
                        {
                            name = "null";
                            exception = "None";
                        }
                        else
                        {
                            name = receivedMessage.Type.ToString();
                            exception = receivedMessage.Exception;
                        }

                        throw new InvalidOperationException(
                            $"On sharded worker '{_dbName}' the {nameof(SubscriptionConnectionServerMessage)} is {name} but expected {nameof(SubscriptionConnectionServerMessage.MessageType.Confirm)}. Exception: {exception}");
                    }

                    // got confirm from subscription connection (shard), now ShardedSubscriptionConnection can send confirm to actual client
                    PublishedShardBatchItem.ConfirmFromShardSubscriptionConnectionTcs.SetResult();
                }
                catch (Exception e)
                {
                    PublishedShardBatchItem.ConfirmFromShardSubscriptionConnectionTcs.SetException(e);
                    if (e is ObjectDisposedException)
                    {
                        // we are disposing
                        return;
                    }
                    CloseTcpClient();
                    throw;
                }
                finally
                {
                    PublishedShardBatchItem = null;
                    incomingBatch.ReturnContext.Dispose();
                }
            }
        }

        private async Task TryPublishBatch(BatchFromServer incomingBatch)
        {
            _processingCts.Token.ThrowIfCancellationRequested();

            // publish batch so ShardedSubscriptionConnection can consume it
            PublishedShardBatchItem = new PublishedShardBatch
            {
                _batchFromServer = incomingBatch,
                SendBatchToClientTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously),
                ConfirmFromShardSubscriptionConnectionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            };

            // Set the start handling worker MRE on ShardedSubscriptionConnection
            _state.HasNewDocuments.SetAndResetAtomically();

            // wait for ShardedSubscriptionConnection to redirect the batch to client worker
            await PublishedShardBatchItem.SendBatchToClientTcs.Task;
        }
    }
}
