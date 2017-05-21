using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Exceptions.Subscriptions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Extensions;
using Raven.Server.ServerWide;
using Raven.Server.Extensions;
using Sparrow.Utils;

namespace Raven.Server.Documents.TcpHandlers
{
    public class SubscriptionConnection : IDisposable
    {
        private static readonly StringSegment DataSegment = new StringSegment("Data");
        private static readonly StringSegment TypeSegment = new StringSegment("Type");


        public readonly TcpConnectionOptions TcpConnection;
        public readonly string ClientUri;
        private readonly MemoryStream _buffer = new MemoryStream();
        private readonly Logger _logger;
        public readonly SubscriptionConnectionStats Stats;
        public readonly CancellationTokenSource CancellationTokenSource;
        private AsyncManualResetEvent _waitForMoreDocuments;

        private SubscriptionConnectionOptions _options;

        public SubscriptionConnectionOptions Options => _options;

        public IDisposable DisposeOnDisconnect;

        public SubscriptionException ConnectionException;

        private static readonly byte[] Heartbeat = Encoding.UTF8.GetBytes("\r\n");

        private SubscriptionState _state;
        private bool _isDisposed;

        public long SubscriptionId => _options.SubscriptionId;
        public SubscriptionOpeningStrategy Strategy => _options.Strategy;
        ServerStore _serverStore;

        public SubscriptionConnection(TcpConnectionOptions connectionOptions, ServerStore serverStore)
        {
            TcpConnection = connectionOptions;
            ClientUri = connectionOptions.TcpClient.Client.RemoteEndPoint.ToString();
            _logger = LoggingSource.Instance.GetLogger<SubscriptionConnection>(connectionOptions.DocumentDatabase.Name);
            _serverStore = serverStore;
            CancellationTokenSource =
                CancellationTokenSource.CreateLinkedTokenSource(TcpConnection.DocumentDatabase.DatabaseShutdown);

            Stats = new SubscriptionConnectionStats();            

            
        }       

        private async Task<bool> ParseSubscriptionOptionsAsync()
        {
            try
            {
                JsonOperationContext context;
                using (TcpConnection.ContextPool.AllocateOperationContext(out context))
                using (var subscriptionCommandOptions = await context.ParseToMemoryAsync(
                    TcpConnection.Stream,
                    "subscription options",
                    BlittableJsonDocumentBuilder.UsageMode.None,
                    TcpConnection.PinnedBuffer))
                {
                    _options = JsonDeserializationServer.SubscriptionConnectionOptions(subscriptionCommandOptions);
                }
            }
            catch (Exception ex)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Failed to parse subscription options document", ex);
                }
                return false;
            }

            return true;
        }

        public async Task<bool> InitAsync()
        {
            if (await ParseSubscriptionOptionsAsync() == false)
                return false;

            if (_logger.IsInfoEnabled)
            {
                _logger.Info(
                    $"Subscription connection for subscription ID: {SubscriptionId} received from {TcpConnection.TcpClient.Client.RemoteEndPoint}");
            }

            try
            {
                TcpConnection.DocumentDatabase.SubscriptionStorage.AssertSubscriptionIdExists(SubscriptionId, TimeSpan.FromSeconds(15));
            }
            catch (SubscriptionDoesNotExistException e)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info("Subscription does not exist", e);
                }
                await WriteJsonAsync(new DynamicJsonValue
                {
                    ["Type"] = "ConnectionStatus",
                    ["Status"] = "NotFound",
                    ["FreeText"] = e.ToString()
                });
                return false;
            }
            _state = TcpConnection.DocumentDatabase.SubscriptionStorage.OpenSubscription(this);
            uint timeout = 16;

            while (true)
            {
                try
                {
                    DisposeOnDisconnect = await _state.RegisterSubscriptionConnection(this, timeout);

                    await WriteJsonAsync(new DynamicJsonValue
                    {
                        ["Type"] = "ConnectionStatus",
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
                            $"Subscription Id {SubscriptionId} from IP {TcpConnection.TcpClient.Client.RemoteEndPoint} starts to wait until previous connection from {_state.Connection?.TcpConnection.TcpClient.Client.RemoteEndPoint} is released");
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
                        ["Type"] = "ConnectionStatus",
                        ["Status"] = "InUse"
                    });
                    return false;
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

        public static void SendSubscriptionDocuments(TcpConnectionOptions tcpConnectionOptions, ServerStore serverStore)
        {
            Task.Run(async () =>
            {
                using (tcpConnectionOptions)
                using (var connection = new SubscriptionConnection(tcpConnectionOptions, serverStore))
                using (tcpConnectionOptions.ConnectionProcessingInProgress("Subscription"))
                {
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
                            connection._logger.Info(
                                $"Failed to process subscription {connection._options?.SubscriptionId} / from client {connection.TcpConnection.TcpClient.Client.RemoteEndPoint}",
                                e);
                        }
                        try
                        {
                            if (connection.ConnectionException != null)
                                return;

                            await connection.WriteJsonAsync(new DynamicJsonValue
                            {
                                ["Type"] = "Error",
                                ["Exception"] = e.ToString()
                            });
                        }
                        catch (Exception)
                        {
                            // ignored
                        }
                    }
                    finally
                    {
                        if (connection._options != null && connection._logger.IsInfoEnabled)
                        {
                            connection._logger.Info(
                                $"Finished proccessing subscription {connection._options?.SubscriptionId} / from client {connection.TcpConnection.TcpClient.Client.RemoteEndPoint}");
                        }

                        if (connection.ConnectionException != null)
                        {
                            try
                            {
                                var status = "None";
                                if (connection.ConnectionException is SubscriptionClosedException)
                                    status = "Closed";

                                await connection.WriteJsonAsync(new DynamicJsonValue
                                {
                                    ["Type"] = "Error",
                                    ["Status"] = status,
                                    ["Exception"] = connection.ConnectionException.ToString()
                                });
                            }
                            catch
                            {
                                // ignored
                            }
                        }
                    }
                }
            });
        }

        private IDisposable RegisterForNotificationOnNewDocuments(SubscriptionCriteria criteria)
        {
            _waitForMoreDocuments = new AsyncManualResetEvent(CancellationTokenSource.Token);

            void RegisterNotification(DocumentChange notification)
            {
                if (notification.CollectionName == criteria.Collection)
                    _waitForMoreDocuments.SetByAsyncCompletion();
            }

            TcpConnection.DocumentDatabase.Changes.OnDocumentChange += RegisterNotification;
            return new DisposableAction(
                    () =>
                    {
                        TcpConnection.DocumentDatabase.Changes.OnDocumentChange -= RegisterNotification;
                    });
        }

        private async Task<SubscriptionConnectionClientMessage> GetReplyFromClient()
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
                    ChangeVector = new ChangeVectorEntry[] {},
                    Type = SubscriptionConnectionClientMessage.MessageType.DisposedNotification
                };
            }
            catch (ObjectDisposedException)
            {
                return new SubscriptionConnectionClientMessage
                {
                    ChangeVector = new ChangeVectorEntry[] {},
                    Type = SubscriptionConnectionClientMessage.MessageType.DisposedNotification
                };
            }
        }

        private async Task ProcessSubscriptionAysnc()
        {
            if (_logger.IsInfoEnabled)
            {
                _logger.Info(
                    $"Starting proccessing documents for subscription {SubscriptionId} received from {TcpConnection.TcpClient.Client.RemoteEndPoint}");
            }

            DocumentsOperationContext dbContext;

            using (DisposeOnDisconnect)
            using (TcpConnection.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out dbContext))
            {
                (var criteria, var startChangeVector) = TcpConnection.DocumentDatabase.SubscriptionStorage.GetCriteriaAndChangeVector(_options.SubscriptionId, dbContext);                
                
                long startEtag = 0;
                
                if (startChangeVector != null && startChangeVector.Length >0)
                {
                    startEtag = GetStartEtagByStartChangeVector(startChangeVector, dbContext, criteria);
                }

                var replyFromClientTask = GetReplyFromClient();                
                var registrenNotificationDisposable = RegisterForNotificationOnNewDocuments(criteria);
                try
                {
                    var patch = SetupFilterScript(criteria);

                    while (CancellationTokenSource.IsCancellationRequested == false)
                    {
                        bool anyDocumentsSentInCurrentIteration = false;
                        using (dbContext.OpenReadTransaction())
                        {                            
                            var documents = TcpConnection.DocumentDatabase.DocumentsStorage.GetDocumentsFrom(
                                dbContext,
                                criteria.Collection,
                                startEtag + 1, 
                                0, 
                                _options.MaxDocsPerBatch);
                            _buffer.SetLength(0);

                            var docsToFlush = 0;

                            var sendingCurrentBatchStopwatch = Stopwatch.StartNew();

                            JsonOperationContext context;
                            using (TcpConnection.ContextPool.AllocateOperationContext(out context))
                            using (var writer = new BlittableJsonTextWriter(context, _buffer))
                            {
                                foreach (var doc in documents)
                                {                                    
                                    using (doc.Data)
                                    {
                                        anyDocumentsSentInCurrentIteration = true;
                                        startEtag = doc.Etag;

                                        BlittableJsonReaderObject transformResult;
                                        if (DocumentMatchCriteriaScript(patch, dbContext, doc, out transformResult) ==
                                            false)
                                        {
                                            // make sure that if we read a lot of irrelevant documents, we send keep alive over the network
                                            if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                                            {
                                                await SendHeartBeat();
                                                sendingCurrentBatchStopwatch.Reset();
                                            }
                                            continue;
                                        }
                                        
                                        writer.WriteStartObject();
                                        writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(TypeSegment));
                                        writer.WriteValue(BlittableJsonToken.String, context.GetLazyStringForFieldWithCaching(DataSegment));
                                        writer.WriteComma();
                                        writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(DataSegment));

                                        if (transformResult != null)
                                        {
                                            var newDoc = new Document
                                            {
                                                Key = doc.Key,
                                                Etag = doc.Etag,
                                                Data = transformResult,
                                                LoweredKey = doc.LoweredKey
                                            };

                                            newDoc.EnsureMetadata();
                                            writer.WriteDocument(dbContext,newDoc);
                                            transformResult.Dispose();
                                        }
                                        else
                                        {
                                            doc.EnsureMetadata();
                                            writer.WriteDocument(dbContext, doc);
                                            doc.Data.Dispose();
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
                                                sendingCurrentBatchStopwatch.Reset();
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
                                    context.Write(writer, new DynamicJsonValue
                                    {
                                        ["Type"] = "EndOfBatch"
                                    });

                                    await FlushDocsToClient(writer, docsToFlush, true);
                                }

                                foreach (var document in documents)
                                {
                                    document.Data.Dispose();
                                }
                            }

                            if (anyDocumentsSentInCurrentIteration == false)
                            {
                                if (await WaitForChangedDocuments(replyFromClientTask))
                                    continue;
                            }

                            SubscriptionConnectionClientMessage clientReply;

                            while (true)
                            {
                                var result = await Task.WhenAny(replyFromClientTask,
                                    TimeoutManager.WaitFor(5000, CancellationTokenSource.Token)).ConfigureAwait(false);
                                CancellationTokenSource.Token.ThrowIfCancellationRequested();
                                if (result == replyFromClientTask)
                                {
                                    clientReply = await replyFromClientTask;
                                    if (clientReply.Type == SubscriptionConnectionClientMessage.MessageType.DisposedNotification)
                                    {
                                        CancellationTokenSource.Cancel();
                                        break;
                                    }
                                    replyFromClientTask = GetReplyFromClient();
                                    break;
                                }
                                await SendHeartBeat();
                            }

                            CancellationTokenSource.Token.ThrowIfCancellationRequested();

                            switch (clientReply.Type)
                            {
                                case SubscriptionConnectionClientMessage.MessageType.Acknowledge:
                                    await TcpConnection.DocumentDatabase.SubscriptionStorage.AcknowledgeBatchProcessed(
                                        _options.SubscriptionId,
                                        clientReply.ChangeVector);
                                    Stats.LastAckReceivedAt = DateTime.UtcNow;
                                    Stats.AckRate.Mark();
                                    await WriteJsonAsync(new DynamicJsonValue
                                    {
                                        ["Type"] = "Confirm",
                                        ["ChangeVector"] = clientReply.ChangeVector.ToJson() // todo: not sure we use this data anyway
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
                finally
                {
                    registrenNotificationDisposable.Dispose();
                }
            }
        }

        private long GetStartEtagByStartChangeVector(ChangeVectorEntry[] startChangeVector, DocumentsOperationContext dbContext, SubscriptionCriteria criteria)
        {
            long startEtag = 0;
            var dbId = TcpConnection.DocumentDatabase.DbId;
            var startChangeVectorAsDictionary = startChangeVector.ToDictionary(x => x.DbId, x => x.Etag);

            if (startChangeVectorAsDictionary.TryGetValue(dbId, out long tempStartEtag))
                startEtag = tempStartEtag;

            var isLastChangeVectorEqualsToChangeVectorMatchingDatabasesEtag = true;

            // first, try to get the first document by etag, in case we continued from the point we stopped
            if (startEtag != 0)
            {
                using (dbContext.OpenReadTransaction())
                {
                    var doc = TcpConnection.DocumentDatabase.DocumentsStorage.GetDocumentsFrom(dbContext, startEtag, 0, 1).FirstOrDefault();

                    if (doc != null)
                    {
                        if (startChangeVector.Length == doc.ChangeVector.Length)
                        {
                            foreach (var changeVectorItem in doc.ChangeVector)
                            {
                                if (startChangeVectorAsDictionary.TryGetValue(changeVectorItem.DbId, out long curEtag) == false
                                    || changeVectorItem.Etag != curEtag)
                                {
                                    isLastChangeVectorEqualsToChangeVectorMatchingDatabasesEtag = false;
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            if (isLastChangeVectorEqualsToChangeVectorMatchingDatabasesEtag == false)
            {
                startEtag = 0;
                using (dbContext.OpenReadTransaction())
                {
                    var highestEtag = TcpConnection.DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(dbContext, criteria.Collection);

                    long low = 0, midpoint = 0;
                    long high = highestEtag;

                    while (low <= high)
                    {
                        midpoint = low + (high - low) / 2;

                        var curDocument = TcpConnection.DocumentDatabase.DocumentsStorage.GetDocumentsFrom(dbContext, midpoint).FirstOrDefault();


                        var conflictStatus = ConflictsStorage.GetConflictStatus(
                            remote: curDocument.ChangeVector,
                            local: startChangeVector);

                        // meaning startCHangeVector >= curDocument.Changevector
                        if (conflictStatus == ConflictsStorage.ConflictStatus.AlreadyMerged)
                        {
                            // as long as startChangeVector is >= then current document's change vector, we take it
                            startEtag = curDocument.Etag;
                            if (curDocument.ChangeVector.EqualTo(startChangeVector))
                                break;
                            // meaning startChangeVector is greater
                            else
                            {
                                low = midpoint + 1;
                            }
                        }
                        // we want to get to a point before the conflict or where curDocument.ChangeVector is greater
                        else
                        {
                            high = midpoint - 1;
                        }
                    }
                }
            }
            return startEtag;
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
                var resultingTask = await Task.WhenAny(hasMoreDocsTask, pendingReply, TimeoutManager.WaitFor(3000)).ConfigureAwait(false);

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

        private bool DocumentMatchCriteriaScript(SubscriptionPatchDocument patch, DocumentsOperationContext dbContext,
            Document doc, out BlittableJsonReaderObject transformResult)
        {
            transformResult = null;
            if (patch == null)
                return true;


            try
            {
                return patch.MatchCriteria(dbContext, doc, out transformResult);
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
// ignored
            }

            CancellationTokenSource.Dispose();
        }
    }
}