// -----------------------------------------------------------------------
//  <copyright file="ShardedSubscriptionConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
// ReSharper disable AccessToDisposedClosure

namespace Raven.Server.Documents.ShardedTcpHandlers
{
    public class ShardedSubscriptionConnection : SubscriptionConnectionBase, IDisposable
    {
        private readonly MemoryStream _buffer = new MemoryStream();
        private readonly List<SubscriptionShardHolder> _shardWorkers = new List<SubscriptionShardHolder>();
        private readonly BlockingCollection<ShardedBatchHolder> _sharedBatchItems = new BlockingCollection<ShardedBatchHolder>();
        private readonly TimeSpan _rejectedReconnectTimeout = TimeSpan.FromMinutes(15); // TODO: egor create configuration

        private HashSet<string> _rejectedShards = new HashSet<string>();
        private bool _isDisposed;

        private ShardedSubscriptionConnection(ServerStore serverStore, TcpConnectionOptions tcpConnectionOptions, IDisposable tcpConnectionDisposable, JsonOperationContext.MemoryBuffer buffer)
            : base(tcpConnectionOptions, serverStore, buffer, tcpConnectionDisposable)
        {
        }
  
        public static void SendShardedSubscriptionDocuments(ServerStore server, TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer)
        {
            var remoteEndPoint = tcpConnectionOptions.TcpClient.Client.RemoteEndPoint;

            var tcpConnectionDisposable = tcpConnectionOptions.ConnectionProcessingInProgress("ShardedSubscription");
            try
            {
                var connection = new ShardedSubscriptionConnection(server, tcpConnectionOptions, tcpConnectionDisposable, buffer);
                try
                {
                    Task.Run(async () =>
                    {
                        using (tcpConnectionOptions)
                        using (tcpConnectionDisposable)
                        using (connection)
                        {
                            connection._lastConnectionStats = new SubscriptionConnectionStatsAggregator(_connectionStatsId, null);
                            connection._connectionScope = connection._lastConnectionStats.CreateScope();
                            connection._pendingConnectionScope = connection._connectionScope.For(SubscriptionOperationScope.ConnectionPending);
                            try
                            {
                                try
                                {
                                    // setup connect with timeout
                                    //var t = Task.Run(() => connection.TryReconnectRejectedShards());
                                    // try connect subscription
                                    await connection.InitShardsWorkers();
                                    await connection.ProcessBatchFromShardsWorkers();
                                }
                                catch (SubscriptionInvalidStateException)
                                {
                                    connection._pendingConnectionScope.Dispose();
                                    throw;
                                }

                            }
                            catch (Exception e)
                            {
                                connection._connectionScope.RecordException(e is SubscriptionInUseException ? SubscriptionError.ConnectionRejected : SubscriptionError.Error, e.Message);

                                var errorMessage = $"Failed to process sharded subscription {connection.SubscriptionId} / from client {remoteEndPoint}";
                                connection.AddToStatusDescription($"{errorMessage}. Sending response to client");
                                if (connection._logger.IsInfoEnabled)
                                    connection._logger.Info(errorMessage, e);

                                try
                                {
                                    switch (e)
                                    {
                                        case DatabaseDoesNotExistException _:
                                        case SubscriptionChangeVectorUpdateConcurrencyException _:
                                            await ReportExceptionToClient(server, tcpConnectionOptions, connection, e, connection._logger);
                                            break;
                                        case RavenException _:
                                        case AllTopologyNodesDownException _:
                                            // could not connect shard, we report SubscriptionInvalidStateException so the worker throws
                                            //TODO: egor normal expcetion msg
                                            await ReportExceptionToClient(server, tcpConnectionOptions, connection, new SubscriptionInvalidStateException("test", e), connection._logger);
                                            break;
                                        default:
                                            await ReportExceptionToClient(server, tcpConnectionOptions, connection, e, connection._logger);
                                            break;
                                    }
                                }
                                catch (Exception)
                                {
                                    // ignored
                                }
                            }
                            finally
                            {
                                connection.AddToStatusDescription("Finished processing sharded subscription.");
                                if (connection._logger.IsInfoEnabled)
                                {
                                    connection._logger.Info($"Finished processing sharded subscription '{connection.SubscriptionId}' / from client '{remoteEndPoint}'");
                                }
                            }
                        }
                    });
                }
                catch (Exception)
                {
                    connection?.Dispose();
                    throw;
                }
            }
            catch (Exception)
            {
                tcpConnectionDisposable?.Dispose();
                throw;
            }
        }

        private async Task InitShardsWorkers()
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                (var options, long? id) = await ParseSubscriptionOptionsAsync(context, _serverStore, TcpConnection, _copiedBuffer.Buffer, TcpConnection.ShardedContext.DatabaseName, CancellationTokenSource.Token);
                _options = options;
                if (id.HasValue)
                    SubscriptionId = id.Value;
            }

            for (int i = 0; i < TcpConnection.ShardedContext.Count; i++)
            {
                var re = TcpConnection.ShardedContext.RequestExecutors[i];
                if (re.Topology == null)
                    await re.WaitForTopologyUpdate(re._firstTopologyUpdate);

                var shard = TcpConnection.ShardedContext.GetShardedDatabaseName(i);
                var store = new DocumentStore { Database = shard, Urls = new[] { re.Url } };
                store.Initialize();

                var shardWorker = new SubscriptionWorker<dynamic>(_options, store, shard);

                var worker = new SubscriptionShardHolder
                {
                    Store = store,
                    Worker = shardWorker,
                    Database = shard,
                };

                var pullingTask = shardWorker.Run(batch =>
                {
                    var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
                    var returnCtx = _serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx);
                    var batchHolder = new ShardedBatchHolder { ReturnContext = returnCtx, Context = ctx, Holder = worker, Batch = batch.Clone(ctx), ClientAckTcs = tcs };
                    _sharedBatchItems.Add(batchHolder, CancellationTokenSource.Token);
                    tcs.Task.Wait(CancellationTokenSource.Token);
                    // TODO: egor go to other shards and try to retrieve missing includes
                }, CancellationTokenSource.Token);

                worker.PullingTask = pullingTask;
                worker.IsFaulted = pullingTask.IsFaulted;
                _shardWorkers.Add(worker);
            }

            //TODO: egor cehck if none of the workers faulted and throw if needed
            // confirm connection status
            await WriteJsonAsync(new DynamicJsonValue
            {
                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
            }, TcpConnection);
        }

        private async Task ProcessBatchFromShardsWorkers()
        {
            using (DisposeOnDisconnect)
            {
                var replyFromClientTask = GetReplyFromClientAsync(TcpConnection, _copiedBuffer.Buffer, _isDisposed);
                while (CancellationTokenSource.IsCancellationRequested == false)
                {
                    if (_sharedBatchItems.TryTake(out ShardedBatchHolder batchHolder, millisecondsTimeout: 3000, CancellationTokenSource.Token) == false)
                    {
                        await SendHeartBeat("Waited for 3000ms for batch from shard workers");
                        continue;
                    }

                    using (batchHolder.ReturnContext)
                    using (batchHolder.Context)
                    {
                        // try to send batch
                        await WriteBatchToClientAndAck(batchHolder.Batch);

                        // finished batch lets wait for client worker ack
                        replyFromClientTask = await WaitForClientAckNew(replyFromClientTask, batchHolder.Holder, batchHolder.ClientAckTcs);
                    }
                }
            }
        }

        private async Task WriteBatchToClientAndAck(SubscriptionBatch<dynamic> batch)
        {
            AddToStatusDescription("Starting to send documents from shards to client");
            int docsToFlush = 0;
            var sendingCurrentBatchStopwatch = Stopwatch.StartNew();
            using (TcpConnection.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, _buffer))
            {
                foreach (var doc in batch.Items)
                {
                    SubscriptionConnection.WriteDocumentOrException(context, writer, document: null, doc.RawResult, doc.RawMetadata, doc.ExceptionMessage, doc.Id, null, null, null);
                    docsToFlush++;

                    if (await SubscriptionConnection.FlushBatchIfNeeded(sendingCurrentBatchStopwatch, SubscriptionId, writer, _buffer, TcpConnection, Stats, _logger, docsToFlush, CancellationTokenSource.Token) == false)
                        continue;

                    docsToFlush = 0;
                    sendingCurrentBatchStopwatch.Restart();
                }

                //TODO: egor Handle includes

                SubscriptionConnection.WriteEndOfBatch(writer);

                AddToStatusDescription("Flushing docs collected from shards to client");
                await SubscriptionConnection.FlushDocsToClient(SubscriptionId, writer, _buffer, TcpConnection, Stats, _logger, docsToFlush, endOfBatch: true, CancellationTokenSource.Token);
            }
        }

        private async Task<Task<SubscriptionConnectionClientMessage>> WaitForClientAckNew(Task<SubscriptionConnectionClientMessage> replyFromClientTask, SubscriptionShardHolder holder, TaskCompletionSource<string> tcs)
        {
            SubscriptionConnectionClientMessage clientReply;
            while (true)
            {
                    var result = await Task.WhenAny(replyFromClientTask, TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(5000), CancellationTokenSource.Token)).ConfigureAwait(false);
                    CancellationTokenSource.Token.ThrowIfCancellationRequested();

                    if (result == replyFromClientTask)
                    {
                        clientReply = await replyFromClientTask;

                        if (clientReply.Type != SubscriptionConnectionClientMessage.MessageType.DisposedNotification)
                            replyFromClientTask = GetReplyFromClientAsync(TcpConnection, _copiedBuffer.Buffer, _isDisposed);

                        break;
                    }

                    await SendHeartBeat("Waiting for client ACK");
            }

            CancellationTokenSource.Token.ThrowIfCancellationRequested();

            switch (clientReply.Type)
            {
                case SubscriptionConnectionClientMessage.MessageType.Acknowledge:
                    tcs.TrySetResult(clientReply.ChangeVector);
                    var subsAck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    holder.Worker.AfterAcknowledgment += _ =>
                    {
                        subsAck.TrySetResult();
                        return Task.CompletedTask;
                    };

                    // wait for shard subs worker to confirm, then confirm to actual client subs worker
                    subsAck.Task.Wait(CancellationTokenSource.Token);
                    await WriteJsonAsync(new DynamicJsonValue
                    {
                        [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.Confirm)
                    }, TcpConnection);
                    break;

                // precaution, should not reach this case...
                case SubscriptionConnectionClientMessage.MessageType.DisposedNotification:
                    CancellationTokenSource.Cancel();
                    break;

                default:
                    throw new ArgumentException($"Unknown message type from client {clientReply.Type}");
            }
           
            return replyFromClientTask;
        }
        
        public new void Dispose()
        {
            if (_isDisposed)
                return;

            _isDisposed = true;
            CancellationTokenSource.Dispose();
            _sharedBatchItems.CompleteAdding();

            base.Dispose();

            foreach (var batch in _sharedBatchItems)
            {
                batch.ClientAckTcs.TrySetCanceled();
                batch.Context.Dispose();
                batch.ReturnContext.Dispose();
            }

            foreach (var worker in _shardWorkers)
            {
                worker.Dispose();
            }

            _buffer.Dispose();
        }

        private class SubscriptionShardHolder : IDisposable
        {
            public DocumentStore Store;
            public SubscriptionWorker<dynamic> Worker;
            public Task PullingTask;
            public string Database;
            public bool IsFaulted;

            public void Dispose()
            {
                Worker.Dispose();
                Store.Dispose();
            }
        }

        private class ShardedBatchHolder
        {
            public SubscriptionShardHolder Holder;
            public SubscriptionBatch<dynamic> Batch;
            public IDisposable ReturnContext;
            public JsonOperationContext Context;
            public TaskCompletionSource<string> ClientAckTcs;
        }
    }
}
