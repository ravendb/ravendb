using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.TcpHandlers
{
    public class SubscriptionConnection:IDisposable
    {
        public readonly TcpConnectionOptions TcpConnection;
        private readonly MemoryStream _buffer = new MemoryStream();
        private readonly BlittableJsonTextWriter _bufferedWriter;
        private readonly Logger _logger;
        public readonly SubscriptionConnectionStats Stats;
        public readonly CancellationTokenSource CancellationTokenSource;
        private AsyncManualResetEvent _waitForMoreDocuments;

        private SubscriptionConnectionOptions _options;
        
        public IDisposable DisposeOnDisconnect;
        
        public SubscriptionException ConnectionException;

        private static readonly byte[] Heartbeat = Encoding.UTF8.GetBytes("\r\n");
        
        private SubscriptionState _state;
        private bool _isDisposed;

        public long SubscriptionId => _options.SubscriptionId;
        public SubscriptionOpeningStrategy Strategy => _options.Strategy;

        public SubscriptionConnection(TcpConnectionOptions connectionOptions)
        {
            TcpConnection = connectionOptions;
            _bufferedWriter = new BlittableJsonTextWriter(connectionOptions.Context, _buffer);
            _logger = LoggingSource.Instance.GetLogger<SubscriptionConnection>(connectionOptions.DocumentDatabase.Name);

            CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(TcpConnection.DocumentDatabase.DatabaseShutdown);

            Stats = new SubscriptionConnectionStats();
            TcpConnection.GetTypeSpecificStats = GetTypeSpecificStats;
        }

        private void GetTypeSpecificStats(JsonOperationContext context, DynamicJsonValue val)
        {
            var details = TcpConnection.DocumentDatabase.SubscriptionStorage.GetRunningSubscriptionConnectionHistory(context, SubscriptionId);
            val["Details"] = details;
        }

        private async Task<bool> ParseSubscriptionOptionsAsync()
        {
            BlittableJsonReaderObject subscriptionCommandOptions = null;
            try
            {
                subscriptionCommandOptions = await TcpConnection.MultiDocumentParser.ParseToMemoryAsync("subscription options");

                _options = JsonDeserializationServer.SubscriptionConnectionOptions(subscriptionCommandOptions);
            }
            catch (Exception ex)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Failed to parse subscription options document", ex);
                }
                return false;
            }
            finally
            {
                subscriptionCommandOptions?.Dispose();
            }

            return true;
        }
        public async Task<bool> InitAsync()
        {
            if (await ParseSubscriptionOptionsAsync() == false)
                return false;

            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Subscription connection for subscription ID: {SubscriptionId} received from {TcpConnection.TcpClient.Client.RemoteEndPoint}");
            }
            
            try
            {
                TcpConnection.DocumentDatabase.SubscriptionStorage.AssertSubscriptionIdExists(SubscriptionId);
            }
            catch (SubscriptionDoesNotExistException e)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info("Subscription does not exist", e);
                }
                await WriteJsonAsync(new DynamicJsonValue
                {
                    ["Type"] = "CoonectionStatus",
                    ["Status"] = "NotFound",
                    ["FreeText"] = e.ToString()
                });
                return false;
            }
            _state = TcpConnection.DocumentDatabase.SubscriptionStorage.OpenSubscription(this);
            var timeout = 0;

            while (true)
            {
                try
                {
                    DisposeOnDisconnect = await _state.RegisterSubscriptionConnection(this,
                        timeout);

                    await WriteJsonAsync(new DynamicJsonValue
                    {
                        ["Type"] = "CoonectionStatus",
                        ["Status"] = "Accepted"
                    });
                    
                    Stats.ConnectedAt = DateTime.UtcNow;

                    return true;
                }
                catch (TimeoutException)
                {
                    if (timeout == 0 && _logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"Subscription Id {SubscriptionId} from IP {TcpConnection.TcpClient.Client.RemoteEndPoint} starts to wait until previous connection from {_state.Connection.TcpConnection.TcpClient.Client.RemoteEndPoint} is released");
                    }
                    timeout = Math.Max(250, _options.TimeToWaitBeforeConnectionRetryMilliseconds/2);
                    await SendHeartBeat();

                }
                catch (SubscriptionInUseException)
                {
                    if (timeout == 0 && _logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"Subscription Id {SubscriptionId} from IP {TcpConnection.TcpClient.Client.RemoteEndPoint} with connection strategy {Strategy} was rejected because previous connection from {_state.Connection.TcpConnection.TcpClient.Client.RemoteEndPoint} has stronger connection strategy ({_state.Connection.Strategy})");
                    }

                    await WriteJsonAsync(new DynamicJsonValue
                    {
                        ["Type"] = "CoonectionStatus",
                        ["Status"] = "InUse"
                    });
                    return false;
                }
            }
        }

        private async Task WriteJsonAsync(DynamicJsonValue value)
        {
            TcpConnection.Context.Write(_bufferedWriter, value);
            _bufferedWriter.Flush();
            await FlushBufferToNetwork();
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
            Task.Run(async () =>
            {
                var connection = new SubscriptionConnection(tcpConnectionOptions);
                tcpConnectionOptions.DisposeOnConnectionClose.Add(connection);
                try
                {
                    if (await connection.InitAsync() == false)
                        return;
                    await connection.ProcessSubscriptionAysnc();
                }
                catch (Exception e)
                {
                    if (connection._logger.IsInfoEnabled)
                    {
                        connection._logger.Info($"Failed to process subscription {connection._options?.SubscriptionId} / from client {connection.TcpConnection.TcpClient.Client.RemoteEndPoint}",
                            e);
                    }
                    try
                    {
                        if (connection.ConnectionException != null)
                            return;
                        using (var writer = new BlittableJsonTextWriter(tcpConnectionOptions.Context, tcpConnectionOptions.Stream))
                        {
                            tcpConnectionOptions.Context.Write(writer, new DynamicJsonValue
                            {
                                ["Type"] = "Error",
                                ["Exception"] = e.ToString()
                            });
                        }
                    }
                    catch (Exception)
                    {
                    }
                }
                finally
                {
                    if (connection._options!= null && connection._logger.IsInfoEnabled)
                    {
                        connection._logger.Info($"Finished proccessing subscription {connection._options?.SubscriptionId} / from client {connection.TcpConnection.TcpClient.Client.RemoteEndPoint}");
                    }

                    if (connection.ConnectionException != null)
                    {
                        try
                        {
                            var status = "None";
                            if (connection.ConnectionException is SubscriptionClosedException)
                                status = "Closed";
                            
                            using (var writer = new BlittableJsonTextWriter(tcpConnectionOptions.Context, tcpConnectionOptions.Stream))
                            {
                                tcpConnectionOptions.Context.Write(writer, new DynamicJsonValue
                                {
                                    ["Type"] = "Error",
                                    ["Status"] = status,
                                    ["Exception"] = connection.ConnectionException.ToString()
                                });
                            }
                        }
                        catch
                        {
                        }
                    }
                    tcpConnectionOptions.Dispose();
                }
            });
        }

        private IDisposable RegisterForNotificationOnNewDocuments(SubscriptionCriteria criteria)
        {
            _waitForMoreDocuments = new AsyncManualResetEvent(CancellationTokenSource.Token);
            Action<DocumentChangeNotification> registerNotification = notification =>
            {
                if (notification.CollectionName == criteria.Collection)
                    _waitForMoreDocuments.SetByAsyncCompletion();

            };
            TcpConnection.DocumentDatabase.Notifications.OnDocumentChange += registerNotification;
            return new DisposableAction(() =>
            {
                TcpConnection.DocumentDatabase.Notifications.OnDocumentChange -= registerNotification;
            });
        }


        private async Task ProcessSubscriptionAysnc()
        {
            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Starting proccessing documents for subscription {SubscriptionId} received from {TcpConnection.TcpClient.Client.RemoteEndPoint}");
            }

            DocumentsOperationContext dbContext;
            using (DisposeOnDisconnect)
            using (TcpConnection.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out dbContext))
            {
                long startEtag;
                SubscriptionCriteria criteria;
                TcpConnection.DocumentDatabase.SubscriptionStorage.GetCriteriaAndEtag(_options.SubscriptionId, dbContext,
                    out criteria, out startEtag);

                var replyFromClientTask = TcpConnection.MultiDocumentParser.ParseToMemoryAsync("client reply");
                using (RegisterForNotificationOnNewDocuments(criteria))
                {
                    var patch = SetupFilterScript(criteria);

                    while (CancellationTokenSource.IsCancellationRequested == false)
                    {
                        dbContext.ResetAndRenew();
                        TcpConnection.Context.ResetAndRenew();

                        bool anyDocumentsSentInCurrentIteration = false;
                        using (dbContext.OpenReadTransaction())
                        {
                            var documents = TcpConnection.DocumentDatabase.DocumentsStorage.GetDocumentsFrom(dbContext,
                                criteria.Collection,
                                startEtag+1, 0, _options.MaxDocsPerBatch);
                            _buffer.SetLength(0);
                            var docsToFlush = 0;

                            var sendingCurrentBatchStopwatch = Stopwatch.StartNew();
                            foreach (var doc in documents)
                            {
                                anyDocumentsSentInCurrentIteration = true;
                                startEtag = doc.Etag;
                                if (DocumentMatchCriteriaScript(patch, dbContext, doc) == false)
                                {
                                    // make sure that if we read a lot of irrelevant documents, we send keep alive over the network
                                    if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                                    {
                                        await SendHeartBeat();
                                        sendingCurrentBatchStopwatch.Reset();
                                    }
                                    continue;
                                }
                                doc.EnsureMetadata();

                                TcpConnection.Context.Write(_bufferedWriter, new DynamicJsonValue
                                {
                                    ["Type"] = "Data",
                                    ["Data"] = doc.Data
                                });
                                docsToFlush++;

                                // perform flush for current batch after 1000ms of running
                                if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                                {
                                    if (docsToFlush > 0)
                                    {
                                        await FlushDocsToClient(docsToFlush);
                                        docsToFlush = 0;
                                        sendingCurrentBatchStopwatch.Reset();
                                    }
                                    else
                                    {
                                        await SendHeartBeat();
                                    }
                                }
                            }

                            if (anyDocumentsSentInCurrentIteration)
                            {
                                TcpConnection.Context.Write(_bufferedWriter, new DynamicJsonValue
                                {
                                    ["Type"] = "EndOfBatch"
                                });
                                
                                await FlushDocsToClient(docsToFlush, true);
                            }

                            if (anyDocumentsSentInCurrentIteration == false)
                            {
                                if (await WaitForChangedDocuments(replyFromClientTask))
                                    continue;
                            }

                            SubscriptionConnectionClientMessage clientReply;

                            while (true)
                            {
                                var result =
                                    await Task.WhenAny(replyFromClientTask, Task.Delay(TimeSpan.FromSeconds(5)));
                                if (result == replyFromClientTask)
                                {
                                    using (var reply = await replyFromClientTask)
                                    {
                                        TcpConnection.RegisterBytesReceived(reply.Size);
                                        clientReply = JsonDeserializationServer.SubscriptionConnectionClientMessage(reply);
                                    }
                                    replyFromClientTask = TcpConnection.MultiDocumentParser.ParseToMemoryAsync("client reply");
                                    break;
                                }
                                await SendHeartBeat();
                            }
                            switch (clientReply.Type)
                            {
                                case SubscriptionConnectionClientMessage.MessageType.Acknowledge:
                                    TcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(_options.SubscriptionId,
                                        clientReply.Etag);
                                    Stats.LastAckReceivedAt = DateTime.UtcNow;
                                    Stats.AckRate.Mark();
                                    await WriteJsonAsync(new DynamicJsonValue
                                    {
                                        ["Type"] = "Confirm",
                                        ["Etag"] = clientReply.Etag
                                    });

                                    break;
                                default:
                                    throw new ArgumentException("Unknown message type from client " +
                                                                clientReply.Type);
                            }
                        }
                    }
                }
            }
        }

        private async Task SendHeartBeat()
        {
            await TcpConnection.Stream.WriteAsync(Heartbeat, 0, Heartbeat.Length);
            TcpConnection.RegisterBytesSent(Heartbeat.Length);
        }

        private async Task FlushDocsToClient(int docsToFlush, bool endOfBatch = false)
        {
            if (_logger.IsInfoEnabled)
            {
                _logger.Info(
                    $"Flushing {docsToFlush} documents for subscription {SubscriptionId} sending to {TcpConnection.TcpClient.Client.RemoteEndPoint} {(endOfBatch?", ending batch":string.Empty)}" );
            }
            
            _bufferedWriter.Flush();
            var bufferSize = _buffer.Length;
            await FlushBufferToNetwork();
            Stats.LastMessageSentAt = DateTime.UtcNow;
            Stats.DocsRate.Mark(docsToFlush);
            Stats.BytesRate.Mark(bufferSize);
            TcpConnection.RegisterBytesSent(bufferSize);
        }

        private async Task<bool> WaitForChangedDocuments(Task pendingReply)
        {
            do
            {
                var hasMoreDocsTask = _waitForMoreDocuments.WaitAsync();
                var resultingTask = await Task.WhenAny(hasMoreDocsTask, pendingReply, Task.Delay(3000));
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

        private bool DocumentMatchCriteriaScript(SubscriptionPatchDocument patch, DocumentsOperationContext dbContext,
            Document doc)
        {
            if (patch == null)
                return true;
            try
            {
                return patch.MatchCriteria(dbContext, doc);
            }
            catch (Exception ex)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info(
                        $"Criteria script threw exception for subscription {_options.SubscriptionId} connected to {TcpConnection.TcpClient.Client.RemoteEndPoint} for document id {doc.Key}",
                        ex);
                }
                return false;
            }
        }

        private SubscriptionPatchDocument SetupFilterScript(SubscriptionCriteria criteria)
        {
            SubscriptionPatchDocument patch = null;

            if (string.IsNullOrWhiteSpace(criteria.FilterJavaScript) == false)
            {
                patch = new SubscriptionPatchDocument(TcpConnection.DocumentDatabase, criteria.FilterJavaScript);
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
            }
        }
    }
}