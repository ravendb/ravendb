using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class SubscriptionWebSocketConnection:IDisposable
    {
        private readonly WebSocket _webSocket;
        private readonly IDisposable _contextPoolDisposable;
        private readonly DocumentsOperationContext _context;
        private readonly JsonParserState _ackParserState;
        private readonly MemoryStream _ms;
        private readonly BlittableJsonTextWriter _writer;
        private readonly UnmanagedJsonParser _jsonParser;
        private readonly DocumentDatabase _database;
        private SubscriptionConnectionOptions _options;
        private readonly CancellationTokenSource _linkedCancellationTokenSource;
        private ArraySegment<byte> _clientAckBuffer;
        private SubscriptionConnectionState _state;
        protected static readonly ILog Log = LogManager.GetLogger(typeof(SubscriptionWebSocketConnection).FullName);

        public SubscriptionWebSocketConnection(DocumentsContextPool contextPool, WebSocket webSocket, DocumentDatabase database)
        {
            _database = database;
            _contextPoolDisposable = contextPool.AllocateOperationContext(out _context);
            _webSocket = webSocket;
            _ackParserState = new JsonParserState();
            _ms = new MemoryStream();

            _writer = new BlittableJsonTextWriter(_context, _ms);
            _jsonParser = new UnmanagedJsonParser(_context, _ackParserState, string.Empty);
            _innerCancellationToken = new CancellationTokenSource();
            
            _linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown, _innerCancellationToken.Token);
            _clientAckBuffer = new ArraySegment<byte>(new byte [4096]);

            _listeningAMRE = new AsyncManualResetEvent(_database.DatabaseShutdown);

        }

        public async Task InitConnection(long? id, string connection, string strategy, int? maxDocsPerBatch, int? maxBatchSize)
        {
            try
            {
                _options = ParseOptions(id, connection, strategy, maxDocsPerBatch, maxBatchSize);

                // verify connection is not closed
                _database.SubscriptionStorage.AssertSubscriptionConfigExists(_options.SubscriptionId);
            }
            catch (SubscriptionDoesNotExistException ex)
            {
                await WriteDynamicJsonToWebsocket(new DynamicJsonValue
                {
                    ["Type"] = "CoonectionStatus",
                    ["Status"] = ex.Message
                });
                throw;
            }

            _options.CancellationTokenSource = _linkedCancellationTokenSource;

            _state = _database.SubscriptionStorage.OpenSubscription(_options);

            var timeout = 0;
            while (true)
            {
                try
                {
                    _options.DisposeOnDisconnect = await _state.RegisterSubscriptionConnection(_options, timeout);

                    await WriteDynamicJsonToWebsocket(new DynamicJsonValue
                    {
                        ["Type"] = "CoonectionStatus",
                        ["Status"] = "Accepted"
                    });

                    return;
                }
                catch (TimeoutException)
                {
                    timeout = 500;
                    await SendHeartBeat();
                }
                catch (SubscriptionInUseException)
                {
                    await WriteDynamicJsonToWebsocket(new DynamicJsonValue
                    {
                        ["Type"] = "CoonectionStatus",
                        ["Status"] = "InUse"
                    });

                    // uncomment this when websockets mutual closure problem is resolved in rc4
                    //await
                    //    _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "InUse",
                    //        _linkedCancellationTokenSource.Token);
                    throw;
                }
            }
        }


        private AsyncManualResetEvent _listeningAMRE;


        public async Task Proccess()
        {
            try
            {
                _listenOnWebSocketsTask = ListenOnWebsocket();
                var waitForMoreDocuments = new AsyncManualResetEvent(_linkedCancellationTokenSource.Token);
                long lastEtagSentToClient = 0;

                var id = _options.SubscriptionId;
            
                long startEtag;
                SubscriptionCriteria criteria;
                _database.SubscriptionStorage.GetCriteriaAndEtag(id, _context, out criteria, out startEtag);
            

                Action<DocumentChangeNotification> registerNotification = notification =>
                {
                    if (notification.CollectionName == criteria.Collection)
                        waitForMoreDocuments.SetByAsyncCompletion();

                };
                _database.Notifications.OnDocumentChange += registerNotification;
                try
                {
                    int skipNumber = 0;
                    SubscriptionPatchDocument spd = null;

                    if (string.IsNullOrWhiteSpace(criteria.FilterJavaScript) == false)
                    {
                        spd = new SubscriptionPatchDocument(_database, criteria.FilterJavaScript );
                    }

                    while (true)
                    {
                        _linkedCancellationTokenSource.Token.ThrowIfCancellationRequested();
                        int documentsSent = 0;

                        var hasDocuments = false;
                        using (_context.OpenReadTransaction())
                        {
                            var documents = _database.DocumentsStorage.GetDocumentsAfter(_context,
                                criteria.Collection,
                                startEtag + 1, 0, _options.MaxDocsPerBatch);

                            foreach (var doc in documents)
                            {
                                _linkedCancellationTokenSource.Token.ThrowIfCancellationRequested();
                                hasDocuments = true;
                                startEtag = doc.Etag;
                                var matchesCriteria = false;

                                try
                                {
                                    matchesCriteria = spd?.MatchCriteria(_context, doc) ?? true;
                                }
                                catch (Exception ex)
                                {
                                    // TODO: should this be filtered / not filtered?

                                    Log.ErrorException($"Criteria script threw exception for subscription {this._options.SubscriptionId} for document id {doc.Key}",ex);
                                }

                                if (matchesCriteria == false)
                                {
                                    if (skipNumber++ % _options.MaxDocsPerBatch == 0)
                                    {
                                        await SendHeartBeat();
                                    }
                                    continue;
                                }
                                documentsSent++;
                                doc.EnsureMetadata();

                                _context.Write(_writer, new DynamicJsonValue
                                {
                                    ["Type"] = "Data",
                                    ["Data"] = doc.Data
                                });
                                lastEtagSentToClient = doc.Etag;
                                doc.Data.Dispose();
                            }
                        }
                        _linkedCancellationTokenSource.Token.ThrowIfCancellationRequested();
                        _writer.Flush();
                        await FlushStreamToClient();
                        _database.SubscriptionStorage.UpdateSubscriptionTimes(id, updateLastBatch: true, updateClientActivity: false);
                        
                        if (hasDocuments == false)
                        {
                            Task<bool> waitForMoreTasks;
                            do
                            {
                                _linkedCancellationTokenSource.Token.ThrowIfCancellationRequested();
                                waitForMoreTasks = waitForMoreDocuments.WaitAsync(TimeSpan.FromSeconds(3));
                                await SendHeartBeat();
                            }
                            while (await waitForMoreTasks == false);

                            waitForMoreDocuments.Reset();
                            continue;
                        }

                        if (documentsSent > 0)
                        {
                            long recentLasAcceptedEtag = 0;
                            while ((recentLasAcceptedEtag = Volatile.Read(ref _lastEtagAcceptedFromClient) )< lastEtagSentToClient)
                            {
                                _linkedCancellationTokenSource.Token.ThrowIfCancellationRequested();
                                var waitForAckTask = _listeningAMRE.WaitAsync();
                                while (await Task.WhenAny(waitForAckTask, Task.Delay(TimeSpan.FromSeconds(5)))!= waitForAckTask)
                                {
                                    _linkedCancellationTokenSource.Token.ThrowIfCancellationRequested();
                                    await SendHeartBeat();
                                }

                                recentLasAcceptedEtag = Volatile.Read(ref _lastEtagAcceptedFromClient);
                                _database.SubscriptionStorage.AcknowledgeBatchProcessed(id, recentLasAcceptedEtag);
                            }
                        }
                    }
                }
                finally
                {
                    _database.Notifications.OnDocumentChange -= registerNotification;
                }
            }
            finally
            {
                _state.EndConnection();
                _options.DisposeOnDisconnect.Dispose();

                if (_webSocket.State == WebSocketState.Open)
                {
                    try
                    {

                        using (_context.OpenReadTransaction())
                        {
                            _context.Write(_writer, new DynamicJsonValue
                            {
                                ["Type"] = "Data",
                                ["Status"] = "Terminated"
                            });
                              
                        }

                        _writer.Flush();
                        await FlushStreamToClient();

                        //await WriteDynamicJsonToWebsocket(new DynamicJsonValue
                        //{
                        //    ["Type"] = "ConnectionState",
                        //    ["Status"] = "Terminated"
                        //});
                    }
                    catch(Exception ex)
                    {
                        // nothing to do
                    }
                }

            }
        }

        private long _lastEtagAcceptedFromClient;
        private string _lastMessageType;
        private string _lastMessageData;
        private CancellationTokenSource _innerCancellationToken;
        private Task _listenOnWebSocketsTask;

        private async Task ListenOnWebsocket()
        {
            while (_linkedCancellationTokenSource.IsCancellationRequested == false)
            {
                using (
                    var builder = new BlittableJsonDocumentBuilder(_context, BlittableJsonDocumentBuilder.UsageMode.None,
                        string.Empty, _jsonParser, _ackParserState))
                using (var reader = await GetReaderFromWebsocketWOKA(builder))
                {
                    string messageType;
                    if (reader.TryGet("Type", out messageType) == false)
                    {
                        // ReSharper disable once NotResolvedInText
                        throw new ArgumentNullException("Type field expected subscription response message");
                    }
                    _lastMessageType = messageType;
                    switch (messageType)
                    {
                        case "ACK":
                            long lastEtagAcceptedFromClient;
                            if (reader.TryGet("Data", out lastEtagAcceptedFromClient) == false)
                                // ReSharper disable once NotResolvedInText
                                throw new ArgumentNullException("Did not receive Data field in subscription ACK message");
                            Volatile.Write(ref _lastEtagAcceptedFromClient, lastEtagAcceptedFromClient);
                            _listeningAMRE.SetByAsyncCompletion();
                            break;
                        case "ConnectionTermination":
                            string terminationReason;
                            reader.TryGet("Data", out terminationReason);
                            _lastMessageData = terminationReason;
                            _listeningAMRE.SetByAsyncCompletion();
                            _linkedCancellationTokenSource.Cancel();
                            break;
                        default:
                            _linkedCancellationTokenSource.Cancel();
                            break;
                    }
                    
                }
            }
        }

        private SubscriptionConnectionOptions ParseOptions(long? id, string connection, string strategyString, int? maxDocsPerBatch, int? maxBatchSize)
        {
            SubscriptionOpeningStrategy strategy;

            if (Enum.TryParse(strategyString, out strategy) == false)
                throw new InvalidOperationException("Strategy was not received");

            if (id == null)
                throw new InvalidOperationException("The query string parameter id is mandatory");

            return new SubscriptionConnectionOptions()
            {
                ConnectionId =  connection,
                SubscriptionId =  id.Value,
                Strategy = strategy,
                MaxDocsPerBatch = maxDocsPerBatch ?? 256,
                MaxBatchSize = maxBatchSize ?? 16 * 1024
            };
        }

        //private async Task<long> GetLastEtagFromWebsocket()
        //{
        //    using (var builder = new BlittableJsonDocumentBuilder(_context, BlittableJsonDocumentBuilder.UsageMode.None, string.Empty, _jsonParser,_ackParserState))
        //    using (var reader = await GetReaderFromWebsocket(builder))
        //    {
        //        string messageType;
        //        if (reader.TryGet("Type", out messageType) == false)
        //        {
        //            // ReSharper disable once NotResolvedInText
        //            throw new ArgumentNullException("Type field expected subscription response message");
        //        }
        //        switch (messageType)
        //        {
        //            case "ACK":
        //                long lastEtagAcceptedFromClient;
        //                if (reader.TryGet("Data", out lastEtagAcceptedFromClient) == false)
        //                    // ReSharper disable once NotResolvedInText
        //                    throw new ArgumentNullException("Did not receive Data field in subscription ACK message");
        //                return lastEtagAcceptedFromClient;
        //            case "ConnectionTermination":
        //                string terminationReason;
        //                reader.TryGet("Data", out terminationReason);

        //                throw new OperationCanceledException(terminationReason);
        //        }
        //        throw new ArgumentNullException($"Illegal message type {messageType} received in client ack message");
        //    }

        //}

        private async Task<WebSocketReceiveResult> ReadFromWebSocketWithKeepAlives()
        {
            var receiveAckTask = _webSocket.ReceiveAsync(_clientAckBuffer, _database.DatabaseShutdown);

            while (await Task.WhenAny(receiveAckTask, Task.Delay(5000)) != receiveAckTask)
            {
                _ms.WriteByte((byte)'\r');
                _ms.WriteByte((byte)'\n');
                // just to keep the heartbeat
                await FlushStreamToClient();
            }

            return await receiveAckTask;
        }

        private async Task<BlittableJsonReaderObject> GetReaderFromWebsocket(BlittableJsonDocumentBuilder builder)
        {
            builder.ReadObject();

            while (builder.Read() == false)
            {
                var result = await ReadFromWebSocketWithKeepAlives();
                _jsonParser.SetBuffer(new ArraySegment<byte>(_clientAckBuffer.Array, 0, result.Count));
            }

            builder.FinalizeDocument();

            return builder.CreateReader();
        }

        private async Task<BlittableJsonReaderObject> GetReaderFromWebsocketWOKA(BlittableJsonDocumentBuilder builder)
        {
            builder.ReadObject();

            while (builder.Read() == false)
            {
                var result = await _webSocket.ReceiveAsync(_clientAckBuffer, _database.DatabaseShutdown);
                _jsonParser.SetBuffer(new ArraySegment<byte>(_clientAckBuffer.Array, 0, result.Count));
            }

            builder.FinalizeDocument();

            return builder.CreateReader();
        }

        private async Task FlushStreamToClient(bool endMessage = false)
        {
            ArraySegment<byte> bytes;
            _ms.TryGetBuffer(out bytes);
            await _webSocket.SendAsync(bytes, WebSocketMessageType.Text, endMessage, _database.DatabaseShutdown);
            _ms.SetLength(0);
        }

        private async Task SendHeartBeat()
        {
            _ms.WriteByte((byte)'\r');
            _ms.WriteByte((byte)'\n');
            // just to keep the heartbeat
            await FlushStreamToClient();
        }

        private async Task WriteDynamicJsonToWebsocket(DynamicJsonValue ackMessage)
        {
            _context.Write(_writer, ackMessage);
            await FlushStreamToClient();
        }

        public void Dispose()
        {
            _jsonParser.Dispose();
            _writer.Dispose();
            _ms.Dispose();
            _contextPoolDisposable.Dispose();
        }
    }

    
}