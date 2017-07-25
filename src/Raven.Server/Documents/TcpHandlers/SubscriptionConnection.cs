using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Exceptions.Subscriptions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Utils;
using Sparrow.Utils;
using System.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Voron;

namespace Raven.Server.Documents.TcpHandlers
{
    public class SubscriptionConnection : IDisposable
    {
        private static readonly StringSegment DataSegment = new StringSegment("Data");
        private static readonly StringSegment ExceptionSegment = new StringSegment("Exception");
        private static readonly StringSegment TypeSegment = new StringSegment("Type");


        public readonly TcpConnectionOptions TcpConnection;
        public readonly string ClientUri;
        private readonly MemoryStream _buffer = new MemoryStream();
        private readonly Logger _logger;
        public readonly SubscriptionConnectionStats Stats;
        public readonly CancellationTokenSource CancellationTokenSource;
        private readonly AsyncManualResetEvent _waitForMoreDocuments;

        private SubscriptionConnectionOptions _options;

        public SubscriptionConnectionOptions Options => _options;

        public IDisposable DisposeOnDisconnect;

        public SubscriptionException ConnectionException;

        private static readonly byte[] Heartbeat = Encoding.UTF8.GetBytes("\r\n");

        private SubscriptionConnectionState _connectionState;
        private bool _isDisposed;

        public long SubscriptionId => _options.SubscriptionId;
        public SubscriptionOpeningStrategy Strategy => _options.Strategy;

        public SubscriptionConnection(TcpConnectionOptions connectionOptions)
        {
            TcpConnection = connectionOptions;
            ClientUri = connectionOptions.TcpClient.Client.RemoteEndPoint.ToString();
            _logger = LoggingSource.Instance.GetLogger<SubscriptionConnection>(connectionOptions.DocumentDatabase.Name);
            CancellationTokenSource =
                CancellationTokenSource.CreateLinkedTokenSource(TcpConnection.DocumentDatabase.DatabaseShutdown);

            _waitForMoreDocuments = new AsyncManualResetEvent(CancellationTokenSource.Token);
            Stats = new SubscriptionConnectionStats();
        }

        private async Task ParseSubscriptionOptionsAsync()
        {
            TransactionOperationContext context;
            using (TcpConnection.DocumentDatabase.ServerStore.ContextPool.AllocateOperationContext(out context))
            using (var subscriptionCommandOptions = await context.ParseToMemoryAsync(
                TcpConnection.Stream,
                "subscription options",
                BlittableJsonDocumentBuilder.UsageMode.None,
                TcpConnection.PinnedBuffer))
            {
                _options = JsonDeserializationServer.SubscriptionConnectionOptions(subscriptionCommandOptions);

                if (string.IsNullOrEmpty(_options.SubscriptionName))
                    return;

                context.OpenReadTransaction();

                var subscriptionItemKey = SubscriptionState.GenerateSubscriptionItemKeyName(TcpConnection.DocumentDatabase.Name, _options.SubscriptionName);
                var translation = TcpConnection.DocumentDatabase.ServerStore.Cluster.Read(context, subscriptionItemKey);
                if (translation == null)
                    throw new SubscriptionClosedException("Cannot find any subscription with the name " + _options.SubscriptionName);

                if (translation.TryGet(nameof(SubscriptionState.SubscriptionId), out long id) == false)
                    throw new SubscriptionClosedException("Could not figure out the subscription id for subscription named " + _options.SubscriptionName);

                if (_options.SubscriptionId > 0 && _options.SubscriptionId != id)
                    throw new SubscriptionClosedException("Subscription named " + _options.SubscriptionName + " has id of " + id + " but the subscription expected " + _options.SubscriptionId);

                _options.SubscriptionId = id;
            }

        }

        public async Task InitAsync()
        {
            await ParseSubscriptionOptionsAsync();

            if (_logger.IsInfoEnabled)
            {
                _logger.Info(
                    $"Subscription connection for subscription ID: {SubscriptionId} received from {TcpConnection.TcpClient.Client.RemoteEndPoint}");
            }
            _options.SubscriptionName = _options.SubscriptionName ?? SubscriptionId.ToString();
            await TcpConnection.DocumentDatabase.SubscriptionStorage.AssertSubscriptionIdIsApplicable(SubscriptionId,_options.SubscriptionName, TimeSpan.FromSeconds(15));

            _connectionState = TcpConnection.DocumentDatabase.SubscriptionStorage.OpenSubscription(this);
            var timeout = TimeSpan.FromMilliseconds(16);

            while (true)
            {
                try
                {
                    DisposeOnDisconnect = await _connectionState.RegisterSubscriptionConnection(this, timeout);

                    await WriteJsonAsync(new DynamicJsonValue
                    {
                        [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                        [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
                    });

                    Stats.ConnectedAt = DateTime.UtcNow;
                    return;
                }
                catch (TimeoutException)
                {
                    if (timeout == TimeSpan.Zero && _logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"Subscription Id {SubscriptionId} from IP {TcpConnection.TcpClient.Client.RemoteEndPoint} starts to wait until previous connection from {_connectionState.Connection?.TcpConnection.TcpClient.Client.RemoteEndPoint} is released");
                    }
                    timeout = TimeSpan.FromMilliseconds(Math.Max(250, (long)_options.TimeToWaitBeforeConnectionRetry.TotalMilliseconds / 2));
                    await SendHeartBeat();
                }
            }
        }

        private async Task WriteJsonAsync(DynamicJsonValue value)
        {

            int writtenBytes;
            using (TcpConnection.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, TcpConnection.Stream))
            {
                context.Write(writer, value);
                writtenBytes = writer.Position;
            }

            await TcpConnection.Stream.FlushAsync();
            TcpConnection.RegisterBytesSent(writtenBytes);
        }

        private async Task FlushBufferToNetwork()
        {
            ArraySegment<byte> bytes;
            _buffer.TryGetBuffer(out bytes);
            await TcpConnection.Stream.WriteAsync(bytes.Array, bytes.Offset, bytes.Count);
            await TcpConnection.Stream.FlushAsync();
            TcpConnection.RegisterBytesSent(bytes.Count);
            _buffer.SetLength(0);
        }

        public static void SendSubscriptionDocuments(TcpConnectionOptions tcpConnectionOptions)
        {
            var remoteEndPoint = tcpConnectionOptions.TcpClient.Client.RemoteEndPoint;

            Task.Run(async () =>
            {
                using (tcpConnectionOptions)
                using (var connection = new SubscriptionConnection(tcpConnectionOptions))
                using (tcpConnectionOptions.ConnectionProcessingInProgress("Subscription"))
                {
                    try
                    {
                        await connection.InitAsync();
                        await connection.ProcessSubscriptionAsync();
                    }
                    catch (Exception e)
                    {                        
                        if (connection._logger.IsInfoEnabled)
                        {
                            connection._logger.Info(
                                $"Failed to process subscription {connection._options?.SubscriptionId} / from client {remoteEndPoint}",
                                e);
                        }
                        try
                        {
                            await ReportExceptionToClient(connection, connection.ConnectionException ?? e);
                        }
                        catch (Exception)
                        {
                            // ignored
                        }
                    }
                    finally
                    {
                        if (connection._logger.IsInfoEnabled)
                        {
                            connection._logger.Info(
                                $"Finished processing subscription {connection._options?.SubscriptionId} / from client {remoteEndPoint}");
                        }
                    }
                }
            });
        }

        private static async Task ReportExceptionToClient(SubscriptionConnection connection, Exception ex)
        {
            try
            {
                if (ex is SubscriptionDoesNotExistException)
                {
                    await connection.WriteJsonAsync(new DynamicJsonValue
                    {
                        [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                        [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.NotFound),
                        [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                        [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                    });
                }
                else if (ex is SubscriptionClosedException)
                {
                    await connection.WriteJsonAsync(new DynamicJsonValue
                    {
                        [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                        [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Closed),
                        [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                        [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                    });
                }
                else if (ex is SubscriptionInvalidStateException)
                {
                    await connection.WriteJsonAsync(new DynamicJsonValue
                    {
                        [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                        [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Invalid),
                        [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                        [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                    });
                }
                else if (ex is SubscriptionInUseException)
                {
                    await connection.WriteJsonAsync(new DynamicJsonValue
                    {
                        [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                        [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.InUse),
                        [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                        [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                    });
                }
                else if (ex is SubscriptionDoesNotBelongToNodeException subscriptionDoesNotBelongException)
                {
                    if (connection._logger.IsInfoEnabled)
                    {
                        connection._logger.Info("Subscription does not belong to current node", ex);
                    }
                    await connection.WriteJsonAsync(new DynamicJsonValue
                    {
                        [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                        [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Redirect),
                        [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                        [nameof(SubscriptionConnectionServerMessage.Data)] = new DynamicJsonValue()
                        {
                            [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RedirectedTag)] = subscriptionDoesNotBelongException.AppropriateNode
                        }
                    });
                }
                else
                {
                    await connection.WriteJsonAsync(new DynamicJsonValue
                    {
                        [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.Error),
                        [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.None),
                        [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                        [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                    });
                }
            }
            catch
            {
                // ignored
            }
        }

        private IDisposable RegisterForNotificationOnNewDocuments(SubscriptionCriteria criteria)
        {
            void RegisterNotification(DocumentChange notification)
            {
                if (notification.CollectionName == criteria.Collection)
                    _waitForMoreDocuments.Set();
            }

            TcpConnection.DocumentDatabase.Changes.OnDocumentChange += RegisterNotification;
            return new DisposableAction(
                    () =>
                    {
                        TcpConnection.DocumentDatabase.Changes.OnDocumentChange -= RegisterNotification;
                    });
        }

        private async Task<SubscriptionConnectionClientMessage> GetReplyFromClientAsync()
        {
            try
            {
                using (TcpConnection.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (var reader = await context.ParseToMemoryAsync(
                    TcpConnection.Stream,
                    "Reply from subscription client",
                    BlittableJsonDocumentBuilder.UsageMode.None,
                    TcpConnection.PinnedBuffer))
                {
                    TcpConnection.RegisterBytesReceived(reader.Size);
                    return JsonDeserializationServer.SubscriptionConnectionClientMessage(reader);
                }
            }
            catch (IOException)
            {
                if (_isDisposed == false)
                    throw;

                return new SubscriptionConnectionClientMessage
                {
                    ChangeVector = null,
                    Type = SubscriptionConnectionClientMessage.MessageType.DisposedNotification
                };
            }
            catch (ObjectDisposedException)
            {
                return new SubscriptionConnectionClientMessage
                {
                    ChangeVector = null,
                    Type = SubscriptionConnectionClientMessage.MessageType.DisposedNotification
                };
            }
        }

        private async Task ProcessSubscriptionAsync()
        {            
            if (_logger.IsInfoEnabled)
            {
                _logger.Info(
                    $"Starting processing documents for subscription {SubscriptionId} received from {TcpConnection.TcpClient.Client.RemoteEndPoint}");
            }
            var subscription = TcpConnection.DocumentDatabase.SubscriptionStorage.GetSubscriptionFromServerStore(_options.SubscriptionName);

            using (DisposeOnDisconnect)
            using (TcpConnection.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext docsContext))
            using (RegisterForNotificationOnNewDocuments(subscription.Criteria))
            {
                var replyFromClientTask = GetReplyFromClientAsync();
                var startEtag = GetStartEtagForSubscription(docsContext, subscription);

                string lastChangeVector = null;

                var patch = SetupFilterScript(subscription.Criteria);
                var fetcher = new SubscriptionDocumentsFetcher(TcpConnection.DocumentDatabase,_options.MaxDocsPerBatch, SubscriptionId, TcpConnection.TcpClient.Client.RemoteEndPoint);
                while (CancellationTokenSource.IsCancellationRequested == false)
                {
                    bool anyDocumentsSentInCurrentIteration = false;
                    using (docsContext.OpenReadTransaction())
                    {
                        var sendingCurrentBatchStopwatch = Stopwatch.StartNew();

                        _buffer.SetLength(0);

                        var docsToFlush = 0;

                        JsonOperationContext context;
                        using (TcpConnection.ContextPool.AllocateOperationContext(out context))
                        using (var writer = new BlittableJsonTextWriter(context, _buffer))
                        {
                            foreach (var result in fetcher.GetDataToSend(docsContext, subscription, patch, startEtag))
                            {
                                startEtag = result.Doc.Etag;
                                lastChangeVector = string.IsNullOrEmpty(subscription.ChangeVector) ? 
                                    result.Doc.ChangeVector :
                                    ChangeVectorUtils.MergeVectors(result.Doc.ChangeVector, subscription.ChangeVector);
                                
                                if (result.Doc.Data == null)
                                {
                                    if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                                    {
                                        await SendHeartBeat();
                                        sendingCurrentBatchStopwatch.Restart();
                                    }
                                        
                                    continue;
                                }
                                
                                anyDocumentsSentInCurrentIteration = true;
                                writer.WriteStartObject();
                                
                                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(TypeSegment));
                                writer.WriteValue(BlittableJsonToken.String, context.GetLazyStringForFieldWithCaching(DataSegment));
                                writer.WriteComma();
                                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(DataSegment));
                                result.Doc.EnsureMetadata();
                                
                                if (result.Exception != null)
                                {
                                    writer.WriteValue(BlittableJsonToken.StartObject, 
                                        docsContext.ReadObject(new DynamicJsonValue()
                                        {
                                            [Raven.Client.Constants.Documents.Metadata.Key] = result.Doc.Data[Raven.Client.Constants.Documents.Metadata.Key]
                                        }, result.Doc.Id)
                                    );
                                    writer.WriteComma();
                                    writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(ExceptionSegment));
                                    writer.WriteValue(BlittableJsonToken.String, context.GetLazyStringForFieldWithCaching(result.Exception.ToString()));
                                }
                                else
                                {
                                    writer.WriteDocument(docsContext, result.Doc);
                                    result.Doc.Data.Dispose();
                                }

                                writer.WriteEndObject();
                                docsToFlush++;

                                // perform flush for current batch after 1000ms of running
                                if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                                {
                                    if (docsToFlush > 0)
                                    {
                                        await FlushDocsToClient(writer, docsToFlush);
                                        docsToFlush = 0;
                                        sendingCurrentBatchStopwatch.Restart();
                                    }
                                    else
                                    {
                                        await SendHeartBeat();
                                    }
                                }
                            }

                            if (anyDocumentsSentInCurrentIteration)
                            {                                
                                context.Write(writer, new DynamicJsonValue
                                {
                                    [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.EndOfBatch)
                                });

                                await FlushDocsToClient(writer, docsToFlush, true);                                
                            }
                        }

                        if (anyDocumentsSentInCurrentIteration == false)
                        {                            
                            await TcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(SubscriptionId,Options.SubscriptionName, startEtag, lastChangeVector);

                            if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                                await SendHeartBeat();

                            long globalEtag = TcpConnection.DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(docsContext, subscription.Criteria.Collection);
                            
                            if (globalEtag > startEtag)
                                continue;

                            if (await WaitForChangedDocuments(replyFromClientTask))
                                continue;
                        }

                        SubscriptionConnectionClientMessage clientReply;

                        while (true)
                        {
                            var result = await Task.WhenAny(replyFromClientTask,
                                    TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(5000), CancellationTokenSource.Token)).ConfigureAwait(false);
                            CancellationTokenSource.Token.ThrowIfCancellationRequested();
                            if (result == replyFromClientTask)
                            {
                                clientReply = await replyFromClientTask;
                                if (clientReply.Type == SubscriptionConnectionClientMessage.MessageType.DisposedNotification)
                                {
                                    CancellationTokenSource.Cancel();
                                    break;
                                }
                                replyFromClientTask = GetReplyFromClientAsync();
                                break;
                            }
                            await SendHeartBeat();
                        }
                        
                        CancellationTokenSource.Token.ThrowIfCancellationRequested();

                        switch (clientReply.Type)
                        {
                            case SubscriptionConnectionClientMessage.MessageType.Acknowledge:
                                await TcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(
                                    Options.SubscriptionId,
                                    Options.SubscriptionName,
                                    startEtag,
                                    lastChangeVector);
                                Stats.LastAckReceivedAt = DateTime.UtcNow;
                                Stats.AckRate.Mark();
                                await WriteJsonAsync(new DynamicJsonValue
                                {
                                    [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.Confirm)
                                });

                                break;

                            //precaution, should not reach this case...
                            case SubscriptionConnectionClientMessage.MessageType.DisposedNotification:
                                CancellationTokenSource.Cancel();
                                break;
                            default:
                                throw new ArgumentException("Unknown message type from client " +
                                                            clientReply.Type);
                        }
                    }
                }
            }
        }

        private long GetStartEtagForSubscription(DocumentsOperationContext docsContext, SubscriptionState subscription)
        {
            using (docsContext.OpenReadTransaction())
            {
                long startEtag = 0;

                if (subscription.LastEtagReachedInServer?.TryGetValue(TcpConnection.DocumentDatabase.DbId.ToString(), out startEtag) == true)
                {
                    return startEtag;
                }

                if (string.IsNullOrEmpty(subscription.ChangeVector))
                {
                    return startEtag;
                }
                return startEtag;
            }
        }

        private async Task SendHeartBeat()
        {
            await TcpConnection.Stream.WriteAsync(Heartbeat, 0, Heartbeat.Length);
            TcpConnection.RegisterBytesSent(Heartbeat.Length);
        }

        private async Task FlushDocsToClient(BlittableJsonTextWriter writer, int flushedDocs, bool endOfBatch = false)
        {
            CancellationTokenSource.Token.ThrowIfCancellationRequested();

            if (_logger.IsInfoEnabled)
            {
                _logger.Info(
                    $"Flushing {flushedDocs} documents for subscription {SubscriptionId} sending to {TcpConnection.TcpClient.Client.RemoteEndPoint} {(endOfBatch ? ", ending batch" : string.Empty)}");
            }

            writer.Flush();
            var bufferSize = _buffer.Length;
            await FlushBufferToNetwork();
            Stats.LastMessageSentAt = DateTime.UtcNow;
            Stats.DocsRate.Mark(flushedDocs);
            Stats.BytesRate.Mark(bufferSize);
            TcpConnection.RegisterBytesSent(bufferSize);
        }

        private async Task<bool> WaitForChangedDocuments(Task pendingReply)
        {
            do
            {
                var hasMoreDocsTask = _waitForMoreDocuments.WaitAsync();
                var resultingTask = await Task.WhenAny(hasMoreDocsTask, pendingReply, TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(3000))).ConfigureAwait(false);

                if (CancellationTokenSource.IsCancellationRequested)
                    return false;
                if (resultingTask == pendingReply)
                    return false;

                if (hasMoreDocsTask == resultingTask)
                {
                    _waitForMoreDocuments.Reset();
                    return true;
                }

                await SendHeartBeat();
            } while (CancellationTokenSource.IsCancellationRequested == false);
            return false;
        }

        private SubscriptionPatchDocument SetupFilterScript(SubscriptionCriteria criteria)
        {
            SubscriptionPatchDocument patch = null;

            if (string.IsNullOrWhiteSpace(criteria.Script) == false)
            {
                patch = new SubscriptionPatchDocument(TcpConnection.DocumentDatabase, criteria.Script);
            }
            return patch;
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;
            _isDisposed = true;
            Stats.Dispose();
            try
            {
                TcpConnection.Dispose();
            }
            catch (Exception)
            {
                // ignored
            }

            CancellationTokenSource.Dispose();
        }
    }
}