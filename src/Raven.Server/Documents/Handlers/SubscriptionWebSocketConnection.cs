using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
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
        private CancellationTokenSource _linkedCancellationTokenSource;
        private CancellationTokenSource _internalCancellationTokenSource;
        private ArraySegment<byte> _clientAckBuffer;
        private SubscriptionConnectionState _state;

        public SubscriptionWebSocketConnection(DocumentsContextPool contextPool, WebSocket webSocket, DocumentDatabase database)
        {
            _database = database;
            _contextPoolDisposable = contextPool.AllocateOperationContext(out _context);
            _webSocket = webSocket;
            _ackParserState = new JsonParserState();
            _ms = new MemoryStream();

            _writer = new BlittableJsonTextWriter(_context, _ms);
            _jsonParser = new UnmanagedJsonParser(_context, _ackParserState, string.Empty);
            _internalCancellationTokenSource = new CancellationTokenSource();
            _linkedCancellationTokenSource =
                CancellationTokenSource.CreateLinkedTokenSource(_internalCancellationTokenSource.Token,
                    _database.DatabaseShutdown);
            _clientAckBuffer = new ArraySegment<byte>(new byte [4096]);
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
                    await SendHeartBeat(_ms, _webSocket).ConfigureAwait(false);
                }
                catch (SubscriptionInUseException ex)
                {
                    await WriteDynamicJsonToWebsocket(new DynamicJsonValue
                    {
                        ["Type"] = "CoonectionStatus",
                        ["Status"] = "InUse"
                    });
                    throw;
                }
            }
        }

        
        public async Task Proccess()
        {

            // todo: consider performing the connection abortion in more gracefull way, allowing waiting for the last batch to complete sending
            // maybe even create a special strategy for "gracefull takeover"
            try
            {
                var waitForMoreDocuments = new AsyncManualResetEvent();
                long lastEtagSentToClient = 0;
                long lastEtagAcceptedFromClient = 0;

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
                                if (MatchCriteria(criteria, doc) == false)
                                {
                                    if (skipNumber++ % _options.MaxDocsPerBatch == 0)
                                    {
                                        _ms.WriteByte((byte)'\r');
                                        _ms.WriteByte((byte)'\n');
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
                        await FlushStreamToClient().ConfigureAwait(false);
                        _database.SubscriptionStorage.UpdateSubscriptionTimes(id, updateLastBatch: true, updateClientActivity: false);
                        _linkedCancellationTokenSource.Token.ThrowIfCancellationRequested();
                        if (hasDocuments == false)
                        {
                            while (await
                                waitForMoreDocuments.WaitAsync(TimeSpan.FromSeconds(5))
                                    .ConfigureAwait(false) == false)
                            {
                                _linkedCancellationTokenSource.Token.ThrowIfCancellationRequested();
                                await SendHeartBeat(_ms, _webSocket).ConfigureAwait(false);
                            }

                            waitForMoreDocuments.Reset();
                            continue;
                        }

                        if (documentsSent > 0)
                        {
                            while (lastEtagAcceptedFromClient < lastEtagSentToClient)
                            {
                                _linkedCancellationTokenSource.Token.ThrowIfCancellationRequested();
                                lastEtagAcceptedFromClient = await GetLastEtagFromWebsocket();
                            }
                        
                            _database.SubscriptionStorage.AcknowledgeBatchProcessed(id, startEtag);
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
                _state.Connection.CancellationTokenSource.Cancel();
                _state.EndConnection();
                _options.DisposeOnDisconnect.Dispose();

                try
                {
                    await WriteDynamicJsonToWebsocket(new DynamicJsonValue
                    {
                        ["Type"] = "ConnectionState",
                        ["Status"] = "Terminated"
                    });
                }
                catch
                {
                    // write to log
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

        private async Task<long> GetLastEtagFromWebsocket()
        {
            using (var builder = new BlittableJsonDocumentBuilder(_context, BlittableJsonDocumentBuilder.UsageMode.None, string.Empty, _jsonParser,_ackParserState))
            using (var reader = await GetReaderFromWebsocket(builder))
            {
                string messageType;
                if (reader.TryGet("Type", out messageType) == false)
                {
                    // ReSharper disable once NotResolvedInText
                    throw new ArgumentNullException("Type field expected subscription response message");
                }
                switch (messageType)
                {
                    case "ACK":
                        long lastEtagAcceptedFromClient;
                        if (reader.TryGet("Data", out lastEtagAcceptedFromClient) == false)
                            // ReSharper disable once NotResolvedInText
                            throw new ArgumentNullException("Did not receive Data field in subscription ACK message");
                        return lastEtagAcceptedFromClient;
                    case "ConnectionTermination":
                        string terminationReason;
                        reader.TryGet("Data", out terminationReason);

                        throw new OperationCanceledException(terminationReason);
                }
                throw new ArgumentNullException($"Illegal message type {messageType} received in client ack message");
            }
            
        }

        private async Task<WebSocketReceiveResult> ReadFromWebSocketWithKeepAlives()
        {
            var receiveAckTask = _webSocket.ReceiveAsync(_clientAckBuffer, _database.DatabaseShutdown);
            while (Task.WhenAny(receiveAckTask, Task.Delay(5000)) != null &&
                (receiveAckTask.IsCompleted || receiveAckTask.IsFaulted ||
                    receiveAckTask.IsCanceled) == false)
            {
                _ms.WriteByte((byte)'\r');
                _ms.WriteByte((byte)'\n');
                // just to keep the heartbeat
                await FlushStreamToClient().ConfigureAwait(false);
            }

            return await receiveAckTask.ConfigureAwait(false);
        }

        private async Task<BlittableJsonReaderObject> GetReaderFromWebsocket(BlittableJsonDocumentBuilder builder)
        {
            builder.ReadObject();

            while (builder.Read() == false)
            {
                var result =
                    await
                        ReadFromWebSocketWithKeepAlives().ConfigureAwait(false);
                _jsonParser.SetBuffer(new ArraySegment<byte>(_clientAckBuffer.Array, 0,
                    result.Count));
            }

            builder.FinalizeDocument();

            var reader2 = builder.CreateReader();
            return reader2;
        }

        private async Task FlushStreamToClient(bool endMessage = false)
        {
            ArraySegment<byte> bytes;
            _ms.TryGetBuffer(out bytes);
            await _webSocket.SendAsync(bytes, WebSocketMessageType.Text, endMessage, _linkedCancellationTokenSource.Token).ConfigureAwait(false);
            _ms.SetLength(0);
        }

        private async Task SendHeartBeat(MemoryStream ms, WebSocket webSocket)
        {
            ms.WriteByte((byte)'\r');
            ms.WriteByte((byte)'\n');
            // just to keep the heartbeat
            await
                FlushStreamToClient()
                    .ConfigureAwait(false);
        }

        private async Task WriteDynamicJsonToWebsocket(DynamicJsonValue ackMessage)
        {
            _context.Write(_writer, ackMessage);
            await FlushStreamToClient().ConfigureAwait(false);
        }

        private bool MatchCriteria(SubscriptionCriteria criteria, Document doc)
        {
            // todo: implement
            return true;
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