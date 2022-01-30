using System;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Tcp;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedTcpHandlers
{
    public class ShardedSubscriptionWorker : AbstractSubscriptionWorker<dynamic>
    {
        private readonly RequestExecutor _shardRequestExecutor;

        public ShardedSubscriptionWorker(SubscriptionWorkerOptions options, string dbName, RequestExecutor re) : base(options, dbName)
        {
            _shardRequestExecutor = re; 
        }

        internal override RequestExecutor GetRequestExecutor()
        {
            return _shardRequestExecutor;
        }

        internal override void GetLocalRequestExecutor(string url, X509Certificate2 cert)
        {
            _subscriptionLocalRequestExecutor?.Dispose();
            _subscriptionLocalRequestExecutor = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(url, _dbName, cert, DocumentConventions.Default);
        }

        internal override bool ShouldUseCompression()
        {
            bool compressionSupport = false;
#if NETCOREAPP3_1_OR_GREATER
            var version = SubscriptionTcpVersion ?? TcpConnectionHeaderMessage.SubscriptionTcpVersion;
            if (version >= 53_000 && (_shardRequestExecutor.Conventions.DisableTcpCompression == false))
                compressionSupport = true;
#endif
            return compressionSupport;
        }

        public class PublishedBatch
        {
            internal BatchFromServer _batchFromServer;
            public TaskCompletionSource SendBatchToClientTcs;
            public TaskCompletionSource ConfirmFromShardSubscriptionConnectionTcs;
            public string LastSentChangeVectorInBatch;
        }

        public PublishedBatch PublishedBatchItem;

        internal override async Task ProcessSubscriptionInternal(JsonContextPool contextPool, Stream tcpStreamCopy, JsonOperationContext.MemoryBuffer buffer, JsonOperationContext context)
        {
            while (_processingCts.IsCancellationRequested == false)
            {
                BatchFromServer incomingBatch = await ReadSingleSubscriptionBatchFromServer(contextPool, tcpStreamCopy, buffer, batch: null).ConfigureAwait(false);
                _processingCts.Token.ThrowIfCancellationRequested();

                try
                {
                    // publish batch so ShardedSubscriptionConnection can consume it
                    PublishedBatchItem = new PublishedBatch
                    {
                        _batchFromServer = incomingBatch,
                        SendBatchToClientTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously),
                        ConfirmFromShardSubscriptionConnectionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
                    };

                    // wait for ShardedSubscriptionConnection to redirect the batch to client worker
                    await PublishedBatchItem.SendBatchToClientTcs.Task;

                    Debug.Assert(PublishedBatchItem.LastSentChangeVectorInBatch != null, "PublishedBatchItem.LastSentChangeVectorInBatch != null");
                    Debug.Assert(tcpStreamCopy != null);

                    // send ack to SubscriptionConnection (to shard)
                    await SendAckAsync(PublishedBatchItem.LastSentChangeVectorInBatch, tcpStreamCopy, context, _processingCts.Token).ConfigureAwait(false);
                    // hard coded to get confirm of applying acknowledge command
                    SubscriptionConnectionServerMessage receivedMessage = await ReadNextObject(context, tcpStreamCopy, buffer).ConfigureAwait(false);
                    _processingCts.Token.ThrowIfCancellationRequested();
                    if (receivedMessage == null || receivedMessage.Type != SubscriptionConnectionServerMessage.MessageType.Confirm)
                    {
                        var name = receivedMessage == null ? "null" : receivedMessage.Type.ToString();
                        throw new InvalidOperationException(
                            $"On sharded worker '{_dbName}' the {nameof(SubscriptionConnectionServerMessage)} is {name} but expected {nameof(SubscriptionConnectionServerMessage.MessageType.Confirm)}.");
                    }

                    // got confirm from subscription connection (shard), now ShardedSubscriptionConnection can send confirm to actual client
                    PublishedBatchItem.ConfirmFromShardSubscriptionConnectionTcs.SetResult();
                }
                catch (Exception e)
                {
                    PublishedBatchItem.ConfirmFromShardSubscriptionConnectionTcs.SetException(e);
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
                    PublishedBatchItem = null;
                    incomingBatch.ReturnContext.Dispose();
                }
            }
        }
    }
}
