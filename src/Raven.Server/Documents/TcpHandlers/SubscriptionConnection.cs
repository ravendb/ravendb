using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Raven.Server.Utils;
using Sparrow.Utils;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Replication;
using Constants = Voron.Global.Constants;
using QueryParser = Raven.Server.Documents.Queries.Parser.QueryParser;

namespace Raven.Server.Documents.TcpHandlers
{
    public class SubscriptionConnection : IDisposable
    {
        private const int WaitForChangedDocumentsTimeoutInMs = 3000;
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

        private SubscriptionWorkerOptions _options;

        public SubscriptionWorkerOptions Options => _options;

        public IDisposable DisposeOnDisconnect;

        public SubscriptionException ConnectionException;

        private static readonly byte[] Heartbeat = Encoding.UTF8.GetBytes("\r\n");

        private SubscriptionConnectionState _connectionState;
        private bool _isDisposed;
        public SubscriptionState SubscriptionState;

        public string Collection, Script;
        public string[] Functions;
        public bool Revisions;

        public long SubscriptionId { get; set; }
        public SubscriptionOpeningStrategy Strategy => _options.Strategy;

        public SubscriptionConnection(TcpConnectionOptions connectionOptions, IDisposable tcpConnectionDisposable, JsonOperationContext.ManagedPinnedBuffer bufferToCopy)
        {
            TcpConnection = connectionOptions;
            _tcpConnectionDisposable = tcpConnectionDisposable;
            ClientUri = connectionOptions.TcpClient.Client.RemoteEndPoint.ToString();
            _logger = LoggingSource.Instance.GetLogger<SubscriptionConnection>(connectionOptions.DocumentDatabase.Name);
            CancellationTokenSource =
                CancellationTokenSource.CreateLinkedTokenSource(TcpConnection.DocumentDatabase.DatabaseShutdown);

            _waitForMoreDocuments = new AsyncManualResetEvent(CancellationTokenSource.Token);
            Stats = new SubscriptionConnectionStats();

            _copiedBuffer = bufferToCopy.Clone(connectionOptions.ContextPool);
        }

        private async Task ParseSubscriptionOptionsAsync()
        {
            using (TcpConnection.DocumentDatabase.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var subscriptionCommandOptions = await context.ParseToMemoryAsync(
                TcpConnection.Stream,
                "subscription options",
                BlittableJsonDocumentBuilder.UsageMode.None,
                _copiedBuffer.Buffer))
            {
                _options = JsonDeserializationServer.SubscriptionConnectionOptions(subscriptionCommandOptions);

                if (string.IsNullOrEmpty(_options.SubscriptionName))
                    return;

                context.OpenReadTransaction();

                var subscriptionItemKey = SubscriptionState.GenerateSubscriptionItemKeyName(TcpConnection.DocumentDatabase.Name, _options.SubscriptionName);
                var translation = TcpConnection.DocumentDatabase.ServerStore.Cluster.Read(context, subscriptionItemKey);
                if (translation == null)
                    throw new SubscriptionDoesNotExistException("Cannot find any subscription with the name " + _options.SubscriptionName);

                if (translation.TryGet(nameof(Client.Documents.Subscriptions.SubscriptionState.SubscriptionId), out long id) == false)
                    throw new SubscriptionClosedException("Could not figure out the subscription id for subscription named " + _options.SubscriptionName);

                SubscriptionId = id;
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
            
            // first, validate details and make sure subscription exists
            SubscriptionState = await TcpConnection.DocumentDatabase.SubscriptionStorage.AssertSubscriptionConnectionDetails(SubscriptionId,_options.SubscriptionName);
            
            _connectionState = TcpConnection.DocumentDatabase.SubscriptionStorage.OpenSubscription(this);
            var timeout = TimeSpan.FromMilliseconds(16);
            
            bool shouldRetry = false;
            
            do
            {
                try
                {
                    DisposeOnDisconnect = await _connectionState.RegisterSubscriptionConnection(this, timeout);
                    shouldRetry = false;                    
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
                    shouldRetry = true;
                }
            } while (shouldRetry);

           

            // refresh subscription data (change vector may have been updated, because in the meanwhile, another subscription could have just completed a batch)            
            SubscriptionState = await TcpConnection.DocumentDatabase.SubscriptionStorage.AssertSubscriptionConnectionDetails(SubscriptionId, _options.SubscriptionName);
            
            (Collection, (Script, Functions), Revisions) = ParseSubscriptionQuery(SubscriptionState.Query);

            try
            {
                await SendNoopAck();
                await WriteJsonAsync(new DynamicJsonValue
                {
                    [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                    [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
                });

                Stats.ConnectedAt = DateTime.UtcNow;
                await TcpConnection.DocumentDatabase.SubscriptionStorage.UpdateClientConnectionTime(SubscriptionState.SubscriptionId,
                    SubscriptionState.SubscriptionName, SubscriptionState.MentorNode);
            }
            catch
            {
                DisposeOnDisconnect.Dispose();
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
            _buffer.TryGetBuffer(out ArraySegment<byte> bytes);
            await TcpConnection.Stream.WriteAsync(bytes.Array, bytes.Offset, bytes.Count);
            await TcpConnection.Stream.FlushAsync();
            TcpConnection.RegisterBytesSent(bytes.Count);
            _buffer.SetLength(0);
        }

        public static void SendSubscriptionDocuments(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            var remoteEndPoint = tcpConnectionOptions.TcpClient.Client.RemoteEndPoint;


            var tcpConnectionDisposable = tcpConnectionOptions.ConnectionProcessingInProgress("Subscription");
            try
            {
                var connection = new SubscriptionConnection(tcpConnectionOptions, tcpConnectionDisposable, buffer);
                try
                {
                    Task.Run(async () =>
                    {
                        using (tcpConnectionOptions)
                        using (tcpConnectionDisposable)
                        using (connection)
                        {
                            try
                            {
                                bool gotSemaphore;
                                if ((gotSemaphore = tcpConnectionOptions.DocumentDatabase.SubscriptionStorage.TryEnterSemaphore()) == false)
                                {
                                    throw new SubscriptionClosedException(
                                        $"Cannot open new subscription connection, max amount of concurrent connections reached ({tcpConnectionOptions.DocumentDatabase.Configuration.Subscriptions.MaxNumberOfConcurrentConnections})");
                                }

                                try
                                {
                                    await connection.InitAsync();
                                    await connection.ProcessSubscriptionAsync();
                                }
                                finally
                                {
                                    if (gotSemaphore)
                                    {
                                        tcpConnectionOptions.DocumentDatabase.SubscriptionStorage.ReleaseSubscriptionsSemaphore();
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                if (connection._logger.IsInfoEnabled)
                                {
                                    connection._logger.Info(
                                        $"Failed to process subscription {connection.SubscriptionId} / from client {remoteEndPoint}",
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
                                        $"Finished processing subscription {connection.SubscriptionId} / from client {remoteEndPoint}");
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

        private static async Task ReportExceptionToClient(SubscriptionConnection connection, Exception ex, int recursionDepth = 0)
        {
            if (recursionDepth == 2)
                return;
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
                        [nameof(SubscriptionConnectionServerMessage.Data)] = new DynamicJsonValue
                        {
                            [nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RedirectedTag)] = subscriptionDoesNotBelongException.AppropriateNode
                        }
                    });
                }
                else if (ex is SubscriptionChangeVectorUpdateConcurrencyException subscriptionConcurrency)
                {
                    if (connection._logger.IsInfoEnabled)
                    {
                        connection._logger.Info("Subscription change vector update concurrency error", ex);
                    }
                    await connection.WriteJsonAsync(new DynamicJsonValue
                    {
                        [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                        [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.ConcurrencyReconnect),
                        [nameof(SubscriptionConnectionServerMessage.Message)] = ex.Message,
                        [nameof(SubscriptionConnectionServerMessage.Exception)] = ex.ToString()
                    });
                }
                else if (ex is RachisApplyException commandExecution && commandExecution.InnerException is SubscriptionException)
                {
                    await ReportExceptionToClient(connection, commandExecution.InnerException, recursionDepth - 1);
                }
                else
                {
                    if (connection._logger.IsInfoEnabled)
                    {
                        connection._logger.Info("Subscription error", ex);
                    }
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

        private IDisposable RegisterForNotificationOnNewDocuments()
        {
            void RegisterNotification(DocumentChange notification)
            {
                if (notification.CollectionName == Collection)
                {
                    try
                    {
                        _waitForMoreDocuments.Set();
                    }
                    catch
                    {
                        if (CancellationTokenSource.IsCancellationRequested)
                            return;
                        try
                        {
                            CancellationTokenSource.Cancel();
                        }
                        catch
                        {
                            // ignored
                        }
                    }
                }
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
                using (var blittable = await context.ParseToMemoryAsync(
                    TcpConnection.Stream,
                    "Reply from subscription client",
                    BlittableJsonDocumentBuilder.UsageMode.None,
                    _copiedBuffer.Buffer))
                {
                    TcpConnection.RegisterBytesReceived(blittable.Size);
                    return JsonDeserializationServer.SubscriptionConnectionClientMessage(blittable);
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
        string _lastChangeVector = null;
        private long _startEtag;
        private SubscriptionPatchDocument _filterAndProjectionScript;
        private SubscriptionDocumentsFetcher _documentsFetcher;
        private readonly IDisposable _tcpConnectionDisposable;
        private (IDisposable ReleaseBuffer, JsonOperationContext.ManagedPinnedBuffer Buffer) _copiedBuffer;

        private async Task ProcessSubscriptionAsync()
        {            
            if (_logger.IsInfoEnabled)
            {
                _logger.Info(
                    $"Starting processing documents for subscription {SubscriptionId} received from {TcpConnection.TcpClient.Client.RemoteEndPoint}");
            }

            using (DisposeOnDisconnect)
            using (RegisterForNotificationOnNewDocuments())
            {
                var replyFromClientTask = GetReplyFromClientAsync();
                
                string subscriptionChangeVectorBeforeCurrentBatch = SubscriptionState.ChangeVectorForNextBatchStartingPoint;

                _startEtag = GetStartEtagForSubscription(SubscriptionState);
                _filterAndProjectionScript = SetupFilterAndProjectionScript();
                _documentsFetcher = new SubscriptionDocumentsFetcher(TcpConnection.DocumentDatabase, _options.MaxDocsPerBatch, SubscriptionId, TcpConnection.TcpClient.Client.RemoteEndPoint, Collection, Revisions, SubscriptionState, _filterAndProjectionScript);

                while (CancellationTokenSource.IsCancellationRequested == false)
                {
                    _buffer.SetLength(0);

                    using (TcpConnection.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext docsContext))
                    {
                        var sendingCurrentBatchStopwatch = Stopwatch.StartNew();

                        var anyDocumentsSentInCurrentIteration = await TrySendingBatchToClient(docsContext, sendingCurrentBatchStopwatch);

                        if (anyDocumentsSentInCurrentIteration == false)
                        {
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info(
                                    $"Did not find any documents to send for subscription {Options.SubscriptionName}");
                            }
                            
                            await TcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(SubscriptionId,
                                Options.SubscriptionName,
                                // if this is a new subscription that we sent anything in this iteration, 
                                // _lastChangeVector is null, so let's not change it
                                _lastChangeVector ?? 
                                    nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange),
                                subscriptionChangeVectorBeforeCurrentBatch);
                            
                            
                            subscriptionChangeVectorBeforeCurrentBatch = _lastChangeVector?? SubscriptionState.ChangeVectorForNextBatchStartingPoint;

                            if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                                await SendHeartBeat();

                            using (docsContext.OpenReadTransaction())
                            {
                                long globalEtag = TcpConnection.DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(docsContext, Collection);

                                if (globalEtag > _startEtag)
                                    continue;
                            }
                            
                            AssertCloseWhenNoDocsLeft();

                            if (await WaitForChangedDocuments(replyFromClientTask))
                                continue;
                        }
                    }

                    (replyFromClientTask, subscriptionChangeVectorBeforeCurrentBatch) = 
                        await WaitForClientAck(replyFromClientTask, subscriptionChangeVectorBeforeCurrentBatch);
                }

                CancellationTokenSource.Token.ThrowIfCancellationRequested();
            }
        }

        private void AssertCloseWhenNoDocsLeft()
        {
            if (_options.CloseWhenNoDocsLeft)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info(
                        $"Closing subscription {Options.SubscriptionName} because did not find any documents to send and it's in '{nameof(SubscriptionWorkerOptions.CloseWhenNoDocsLeft)}' mode");
                }

                throw new SubscriptionClosedException($"Closing subscription {Options.SubscriptionName} because there were no documents left and client connected in '{nameof(SubscriptionWorkerOptions.CloseWhenNoDocsLeft)}' mode");
            }
        }

        private async Task<(Task<SubscriptionConnectionClientMessage> ReplyFromClientTask, string SubscriptionChangeVectorBeforeCurrentBatch)> 
            WaitForClientAck(Task<SubscriptionConnectionClientMessage> replyFromClientTask, 
            string subscriptionChangeVectorBeforeCurrentBatch)
        {
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
                await SendNoopAck();
            }

            CancellationTokenSource.Token.ThrowIfCancellationRequested();

            switch (clientReply.Type)
            {
                case SubscriptionConnectionClientMessage.MessageType.Acknowledge:
                    await TcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(
                        SubscriptionId,
                        Options.SubscriptionName,
                        _lastChangeVector,
                        subscriptionChangeVectorBeforeCurrentBatch);
                    subscriptionChangeVectorBeforeCurrentBatch = _lastChangeVector;
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

            return (replyFromClientTask,subscriptionChangeVectorBeforeCurrentBatch);
        }

        /// <summary>
        /// Iterates on a batch in document collection, process it and send documents if found any match
        /// </summary>
        /// <param name="docsContext"></param>
        /// <param name="sendingCurrentBatchStopwatch"></param>
        /// <returns>Whether succeeded finding any documents to send</returns>
        private async Task<bool> TrySendingBatchToClient(DocumentsOperationContext docsContext, Stopwatch sendingCurrentBatchStopwatch)
        {
            bool anyDocumentsSentInCurrentIteration = false;
            int docsToFlush = 0;
            using (var writer = new BlittableJsonTextWriter(docsContext, _buffer))
            {
                using (docsContext.OpenReadTransaction())
                {
                    foreach (var result in _documentsFetcher.GetDataToSend(docsContext, _startEtag))
                    {
                        _startEtag = result.Doc.Etag;
                        _lastChangeVector = string.IsNullOrEmpty(SubscriptionState.ChangeVectorForNextBatchStartingPoint)
                            ? result.Doc.ChangeVector
                            : ChangeVectorUtils.MergeVectors(result.Doc.ChangeVector, SubscriptionState.ChangeVectorForNextBatchStartingPoint);

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

                        writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(TypeSegment));
                        writer.WriteValue(BlittableJsonToken.String, docsContext.GetLazyStringForFieldWithCaching(DataSegment));
                        writer.WriteComma();
                        writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(DataSegment));
                        result.Doc.EnsureMetadata();
                                               

                        if (result.Exception != null)
                        {
                            if (result.Doc.Data.Modifications != null)
                            {
                                result.Doc.Data = docsContext.ReadObject(result.Doc.Data, "subsDocAferModifications");
                            }

                            var metadata = result.Doc.Data[Client.Constants.Documents.Metadata.Key];
                            writer.WriteValue(BlittableJsonToken.StartObject,
                                docsContext.ReadObject(new DynamicJsonValue
                                {
                                    [Client.Constants.Documents.Metadata.Key] = metadata
                                }, result.Doc.Id)
                            );
                            writer.WriteComma();
                            writer.WritePropertyName(docsContext.GetLazyStringForFieldWithCaching(ExceptionSegment));
                            writer.WriteValue(BlittableJsonToken.String, docsContext.GetLazyStringForFieldWithCaching(result.Exception.ToString()));
                        }
                        else
                        {
                            writer.WriteDocument(docsContext, result.Doc, metadataOnly: false);
                        }

                        writer.WriteEndObject();
                        docsToFlush++;

                        // perform flush for current batch after 1000ms of running or 1 MB
                        if (_buffer.Length > Constants.Size.Megabyte ||
                            sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
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
                }

                if (anyDocumentsSentInCurrentIteration)
                {
                    docsContext.Write(writer, new DynamicJsonValue
                    {
                        [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.EndOfBatch)
                    });

                    await FlushDocsToClient(writer, docsToFlush, true);
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"Finished sending a batch with {docsToFlush} documents for subscription {Options.SubscriptionName}");
                    }
                }
            }
            return anyDocumentsSentInCurrentIteration;
        }

        private long GetStartEtagForSubscription(SubscriptionState subscription)
        {
            using (TcpConnection.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext docsContext))
            using (docsContext.OpenReadTransaction())
            {
                long startEtag = 0;

                if (string.IsNullOrEmpty(subscription.ChangeVectorForNextBatchStartingPoint))
                    return startEtag;

                var changeVector = subscription.ChangeVectorForNextBatchStartingPoint.ToChangeVector();

                var cv = changeVector.FirstOrDefault(x => x.DbId == TcpConnection.DocumentDatabase.DbBase64Id);

                if (cv.DbId == "" && cv.Etag == 0 && cv.NodeTag == 0)
                    return startEtag;

                return cv.Etag;
            }
        }

        private async Task SendHeartBeat()
        {
            try
            {
                await TcpConnection.Stream.WriteAsync(Heartbeat, 0, Heartbeat.Length);
            }
            catch(Exception ex)
            {
                throw new SubscriptionClosedException($"Cannot contact client anymore, closing subscription ({Options?.SubscriptionName})",ex);
            }

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
                var resultingTask = await Task.WhenAny(hasMoreDocsTask, pendingReply, TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(WaitForChangedDocumentsTimeoutInMs))).ConfigureAwait(false);

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
                await SendNoopAck();
            } while (CancellationTokenSource.IsCancellationRequested == false);
            return false;
        }

        private async Task SendNoopAck()
        {
            await TcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(
                SubscriptionId,
                Options.SubscriptionName,
                nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange),
                nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange));
        }

        private SubscriptionPatchDocument SetupFilterAndProjectionScript()
        {
            SubscriptionPatchDocument patch = null;

            if (string.IsNullOrWhiteSpace(Script) == false)
            {
                patch = new SubscriptionPatchDocument(Script, Functions);
            }
            return patch;
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;
            _isDisposed = true;

            using (_copiedBuffer.ReleaseBuffer)
            {
                try
                {
                    _tcpConnectionDisposable?.Dispose();
                }
                catch (Exception)
                {
                    // ignored
                }

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

        public static (string Collection, (string Script, string[] Functions), bool Revisions) ParseSubscriptionQuery(string query)
        {
            var queryParser = new QueryParser();
            queryParser.Init(query);
            var q = queryParser.Parse();

            if (q.IsDistinct)
                throw new NotSupportedException("Subscription does not support distinct queries");
            if (q.From.Index)
                throw new NotSupportedException("Subscription must specify a collection to use");
            if (q.GroupBy != null)
                throw new NotSupportedException("Subscription cannot specify a group by clause");
            if (q.OrderBy != null)
                throw new NotSupportedException("Subscription cannot specify an order by clause");
            if (q.Include != null)
                throw new NotSupportedException("Subscription cannot specify an include clause");
            if (q.UpdateBody != null)
                throw new NotSupportedException("Subscription cannot specify an update clause");

            bool revisions = false;
            if (q.From.Filter is BinaryExpression filter)
            {
                switch (filter.Operator)
                {
                    case OperatorType.Equal:
                    case OperatorType.NotEqual:
                        if(!(filter.Left is FieldExpression fe) || fe.Compound.Count != 1)
                            throw new NotSupportedException("Subscription collection filter can only specify 'Revisions = true'");
                        if (string.Equals(fe.Compound[0], "Revisions", StringComparison.OrdinalIgnoreCase) == false)
                            throw new NotSupportedException("Subscription collection filter can only specify 'Revisions = true'");
                        if (filter.Right is ValueExpression ve)
                        {
                            revisions = filter.Operator == OperatorType.Equal && ve.Value == ValueTokenType.True;
                            if(ve.Value != ValueTokenType.True && ve.Value != ValueTokenType.False)
                                throw new NotSupportedException("Subscription collection filter can only specify 'Revisions = true'");
                        }
                        else
                        {
                            throw new NotSupportedException("Subscription collection filter can only specify 'Revisions = true'");
                        }
                        break;
                    default:
                        throw new NotSupportedException("Subscription must not specify a collection filter (move it to the where clause)");
                }
            }
            else if (q.From.Filter != null)
            {
                throw new NotSupportedException("Subscription must not specify a collection filter (move it to the where clause)");
            }

            var collectionName = q.From.From.FieldValue;
            if (q.Where == null && q.Select == null && q.SelectFunctionBody.FunctionText == null)
                return (collectionName, (null, null), revisions);

            var writer = new StringWriter();

            if (q.From.Alias != null)
            {
                writer.Write("var ");
                writer.Write(q.From.Alias);
                writer.WriteLine(" = this;");
            }
            else if(q.Select != null || q.SelectFunctionBody.FunctionText != null || q.Load != null)
            {
                throw new InvalidOperationException("Cannot specify a select or load clauses without an alias on the query");
            }
            if (q.Load != null)
            {
                Debug.Assert(q.From.Alias != null);
                
                var fromAlias = q.From.Alias.Value;
                foreach (var tuple in q.Load)
                {
                    writer.Write("var ");
                    writer.Write(tuple.Alias);
                    writer.Write(" = loadPath(this,'");
                    var fieldExpression = ((FieldExpression)tuple.Expression);
                    if (fieldExpression.Compound[0] != fromAlias)
                        throw new InvalidOperationException("Load clause can only load paths starting from the from alias: " + fromAlias);
                    writer.Write(fieldExpression.FieldValueWithoutAlias);
                    writer.WriteLine("');");
                }
            }

            if (q.Where != null)
            {
                writer.Write("if (");
                new JavascriptCodeQueryVisitor(writer.GetStringBuilder(), q).VisitExpression(q.Where);
                writer.WriteLine(" )");
                writer.WriteLine("{");
            }

            if (q.SelectFunctionBody.FunctionText != null)
            {
                writer.Write(" return ");
                writer.Write(q.SelectFunctionBody.FunctionText);
                writer.WriteLine(";");
            }
            else if (q.Select != null)
            {
                if (q.Select.Count != 1 || q.Select[0].Expression is MethodExpression == false)
                    throw new NotSupportedException("Subscription select clause must specify an object literal");
                writer.WriteLine();
                writer.Write(" return ");
                new JavascriptCodeQueryVisitor(writer.GetStringBuilder(), q).VisitExpression(q.Select[0].Expression);
                writer.WriteLine(";");
            }
            else
            {
                writer.WriteLine(" return true;");
            }
            writer.WriteLine();

            if (q.Where != null)
                writer.WriteLine("}");

            var script = writer.GetStringBuilder().ToString();

            // verify that the JS code parses
            new Esprima.JavaScriptParser(script).ParseProgram();

            return (collectionName, (script, q.DeclaredFunctions?.Values?.Select(x => x.FunctionText).ToArray() ?? Array.Empty<string>()), revisions);
        }
    }

    public class SubscriptionConnectionDetails
    {
        public string ClientUri { get; set; }
        public SubscriptionOpeningStrategy? Strategy { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ClientUri)] = ClientUri,
                [nameof(Strategy)] = Strategy
            };
        }
    }
}
