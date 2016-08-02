using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Metrics;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.TcpHandlers
{
    public class SubscriptionConnection:IDisposable
    {
        private readonly MemoryStream _buffer = new MemoryStream();
        private readonly Stream _networkStream;
        private readonly DocumentDatabase _database;
        private readonly JsonOperationContext _context;
        private readonly JsonOperationContext.MultiDocumentParser _multiDocumentParser;
        private readonly BlittableJsonTextWriter _bufferedWriter;
        private readonly Logger _logger;
        private readonly BlittableJsonTextWriter _unbufferedWriter;
        public readonly EndPoint ClientEndpoint;
        public readonly SubscriptionConnectionStats Stats;
        public readonly CancellationTokenSource CancellationTokenSource;
        private AsyncManualResetEvent _waitForMoreDocuments;

        private SubscriptionConnectionOptions _options;
        
        public IDisposable DisposeOnDisconnect;
        
        public SubscriptionException ConnectionException;

        private static readonly byte[] Heartbeat = Encoding.UTF8.GetBytes("\r\n");
        
        private SubscriptionState _state;

        
        public long SubscriptionId => _options.SubscriptionId;
        public SubscriptionOpeningStrategy Strategy => _options.Strategy;

        public SubscriptionConnection(Stream networkStream, EndPoint clientEndpoint, DocumentDatabase database, 
            JsonOperationContext context,JsonOperationContext.MultiDocumentParser multiDocumentParser, 
            MetricsScheduler metricsScheduler)
        {
            _networkStream = networkStream;
            _database = database;
            _context = context;
            ClientEndpoint = clientEndpoint;
            _multiDocumentParser = multiDocumentParser;
            _bufferedWriter = new BlittableJsonTextWriter(context, _buffer);
            _unbufferedWriter = new BlittableJsonTextWriter(context, networkStream);
            _logger = database.LoggerSetup.GetLogger<SubscriptionConnection>(database.Name);

            CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);

            Stats = new SubscriptionConnectionStats(metricsScheduler);
        }

        private async Task<bool> ParseSubscriptionOptionsAsync()
        {
            BlittableJsonReaderObject subscriptionCommandOptions = null;
            try
            {
                subscriptionCommandOptions = await _multiDocumentParser.ParseToMemoryAsync("subscription options");

                _options = JsonDeserialization.SubscriptionConnectionOptions(subscriptionCommandOptions);
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
                _logger.Info($"Subscription connection for subscription ID: {SubscriptionId} received from {ClientEndpoint}");
            }
            
            try
            {
                _database.SubscriptionStorage.AssertSubscriptionIdExists(SubscriptionId);
            }
            catch (SubscriptionDoesNotExistException e)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info("Subscription does not exist", e);
                }
                _context.Write(_unbufferedWriter, new DynamicJsonValue
                {
                    ["Type"] = "CoonectionStatus",
                    ["Status"]="NotFound",
                    ["FreeText"] = e.ToString()
                });
                return false;
            }
            _state = _database.SubscriptionStorage.OpenSubscription(this);
            var timeout = 0;
            long connectionAttemptStart = Stopwatch.GetTimestamp();

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
                    Stats.WaitedForConnection = Stopwatch.GetTimestamp() - connectionAttemptStart;
                    Stats.ConnectedAt = Stopwatch.GetTimestamp();

                    return true;
                }
                catch (TimeoutException)
                {
                    if (timeout == 0 && _logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"Subscription Id {SubscriptionId} from IP {this.ClientEndpoint} starts to wait until previous connection from {_state.Connection.ClientEndpoint} is released");
                    }
                    timeout = Math.Max(250, _options.TimeToWaitBeforeConnectionRetryMilliseconds/2);
                    await _networkStream.WriteAsync(Heartbeat, 0, Heartbeat.Length);
                }
                catch (SubscriptionInUseException)
                {
                    if (timeout == 0 && _logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"Subscription Id {SubscriptionId} from IP {this.ClientEndpoint} with connection strategy {this.Strategy} was rejected because previous connection from {_state.Connection.ClientEndpoint} has stronger connection strategy ({_state.Connection.Strategy})");
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
            _context.Write(_bufferedWriter, value);
            _bufferedWriter.Flush();
            await FlushBufferToNetwork();
        }

        private async Task FlushBufferToNetwork()
        {
            ArraySegment<byte> bytes;
            _buffer.TryGetBuffer(out bytes);
            await _networkStream.WriteAsync(bytes.Array, bytes.Offset, bytes.Count);
            await _networkStream.FlushAsync();
            _buffer.SetLength(0);
        }

        public static void SendSubscriptionDocuments(DocumentDatabase database, JsonOperationContext context, NetworkStream stream, EndPoint clientEndpoint, JsonOperationContext.MultiDocumentParser multiDocumentParser, MetricsScheduler metricsScheduler)
        {
            Task.Run(async () =>
            {
                var connection = new SubscriptionConnection(stream, clientEndpoint, database, context, multiDocumentParser,metricsScheduler);
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
                        connection._logger.Info($"Failed to process subscription {connection._options?.SubscriptionId} / from client {connection.ClientEndpoint}",
                            e);
                    }
                    try
                    {
                        if (connection.ConnectionException != null)
                            return;
                        using (var writer = new BlittableJsonTextWriter(context, stream))
                        {
                            context.Write(writer, new DynamicJsonValue
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
                        connection._logger.Info($"Finished proccessing subscription {connection._options?.SubscriptionId} / from client {connection.ClientEndpoint}");
                    }

                    if (connection.ConnectionException != null)
                    {
                        try
                        {
                            var status = "None";
                            if (connection.ConnectionException is SubscriptionClosedException)
                                status = "Closed";
                            
                            using (var writer = new BlittableJsonTextWriter(context, stream))
                            {
                                context.Write(writer, new DynamicJsonValue
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
                    SilentDispose(stream);
                    SilentDispose(context);
                    SilentDispose(connection);
                }
            });
        }

        private static void SilentDispose(IDisposable disposable)
        {
            try
            {
                disposable.Dispose();
            }
            catch
            {
                // ignored
            }
        }

        private IDisposable RegisterForNotificationOnNewDocuments(SubscriptionCriteria criteria)
        {
            _waitForMoreDocuments = new AsyncManualResetEvent(CancellationTokenSource.Token);
            Action<DocumentChangeNotification> registerNotification = notification =>
            {
                if (notification.CollectionName == criteria.Collection)
                    _waitForMoreDocuments.SetByAsyncCompletion();

            };
            _database.Notifications.OnDocumentChange += registerNotification;
            return new DisposableAction(() =>
            {
                _database.Notifications.OnDocumentChange -= registerNotification;
            });
        }


        private async Task ProcessSubscriptionAysnc()
        {
            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Starting proccessing documents for subscription {SubscriptionId} received from {ClientEndpoint}");
            }

            DocumentsOperationContext dbContext;
            using (DisposeOnDisconnect)
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out dbContext))
            {
                long startEtag;
                SubscriptionCriteria criteria;
                _database.SubscriptionStorage.GetCriteriaAndEtag(_options.SubscriptionId, dbContext,
                    out criteria, out startEtag);

                var replyFromClientTask = _multiDocumentParser.ParseToMemoryAsync("client reply");
               
                using (RegisterForNotificationOnNewDocuments(criteria))
                {
                    var patch = SetupFilterScript(criteria);

                    while (CancellationTokenSource.IsCancellationRequested == false)
                    {
                        bool anyDocumentsSentInCurrentIteration = false;
                        using (dbContext.OpenReadTransaction())
                        {
                            var documents = _database.DocumentsStorage.GetDocumentsAfter(dbContext,
                                criteria.Collection,
                                startEtag + 1, 0, _options.MaxDocsPerBatch);
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
                                        await _networkStream.WriteAsync(Heartbeat, 0, Heartbeat.Length);
                                        sendingCurrentBatchStopwatch.Reset();
                                    }
                                    continue;
                                }
                                doc.EnsureMetadata();

                                _context.Write(_bufferedWriter, new DynamicJsonValue
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
                                        await _networkStream.WriteAsync(Heartbeat, 0, Heartbeat.Length);
                                    }
                                }
                            }

                            if (anyDocumentsSentInCurrentIteration)
                            {
                                _context.Write(_bufferedWriter, new DynamicJsonValue
                                {
                                    ["Type"] = "EndOfBatch"
                                });
                                
                                await FlushDocsToClient(docsToFlush, true);
                            }

                            _database.SubscriptionStorage.UpdateSubscriptionTimes(_options.SubscriptionId,
                                updateLastBatch: true, updateClientActivity: false);

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
                                        clientReply = JsonDeserialization.SubscriptionConnectionClientMessage(reply);
                                    }
                                    replyFromClientTask = _multiDocumentParser.ParseToMemoryAsync("client reply");
                                    break;
                                }
                                await _networkStream.WriteAsync(Heartbeat, 0, Heartbeat.Length);
                            }
                            switch (clientReply.Type)
                            {
                                case SubscriptionConnectionClientMessage.MessageType.Acknowledge:
                                    _database.SubscriptionStorage.AcknowledgeBatchProcessed(_options.SubscriptionId,
                                        clientReply.Etag);
                                    Stats.LastAckReceivedAt = Stopwatch.GetTimestamp();
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

        private async Task FlushDocsToClient(int docsToFlush, bool endOfBatch = false)
        {
            if (_logger.IsInfoEnabled)
            {
                _logger.Info(
                    $"Flushing {docsToFlush} documents for subscription {SubscriptionId} sending to {ClientEndpoint} {(endOfBatch?", ending batch":string.Empty)}" );
            }

            if (endOfBatch)

            _bufferedWriter.Flush();
            var bufferSize = _buffer.Length;
            await FlushBufferToNetwork();
            Stats.LastMessageSentAt = Stopwatch.GetTimestamp();
            Stats.DocsRate.Mark(docsToFlush);
            Stats.BytesRate.Mark(bufferSize);
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

                await _networkStream.WriteAsync(Heartbeat, 0, Heartbeat.Length);
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
                        $"Criteria script threw exception for subscription {_options.SubscriptionId} connected to {ClientEndpoint} for document id {doc.Key}",
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
                patch = new SubscriptionPatchDocument(_database, criteria.FilterJavaScript);
            }
            return patch;
        }

        public void Dispose()
        {
            Stats.Dispose();
        }
    }
}