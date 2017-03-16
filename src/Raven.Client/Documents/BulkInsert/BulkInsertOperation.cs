using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Exceptions.BulkInsert;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Server;
using Raven.Client.Util;
using Raven.Client.Util.Sockets;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.BulkInsert
{
    public class BulkInsertOperation : IDisposable
    {
        private readonly IDocumentStore _store;
        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;

        private readonly ClientWebSocket _client;
        private readonly JsonOperationContext _context;
        private Task _previousOperation;
        private readonly MemoryStream _stream = new MemoryStream();
        private readonly BlittableJsonTextWriter _jsonWriter;
        private readonly StreamWriter _entityWriter;
        private readonly DocumentConventions _documentConventions = new DocumentConventions();
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly Task<WebSocketReceiveResult> _answerFromServer;
        private readonly JsonOperationContext.ManagedPinnedBuffer _recieveBuffer;
        private JsonOperationContext.ReturnBuffer _returnReceiveBuffer;
        private readonly IDisposable _releaseOperationContext;

        public BulkInsertOperation(string database, IDocumentStore store, bool isSecure = false)
        {
            _store = store;
            database = database ?? MultiDatabase.GetDatabaseName(store.Url);
            _client = new ClientWebSocket();
            
            var replaceTerm = isSecure ? "wss" : "ws";
            Init($"{store.Url.Replace("http", replaceTerm)}/databases/{database}/bulk_insert");

            _releaseOperationContext = store.GetRequestExecuter(database).ContextPool.AllocateOperationContext(out _context);
            _returnReceiveBuffer = _context.GetManagedBuffer(out _recieveBuffer);
            _answerFromServer = _client.ReceiveAsync(_recieveBuffer.Buffer, _cts.Token);
         
            _entityWriter = new StreamWriter(_stream);
            _jsonWriter = new BlittableJsonTextWriter(_context,_stream);
            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(store.Conventions, entity =>
                AsyncHelpers.RunSync(() => store.Conventions.GenerateDocumentIdAsync(database, entity)));
        }

        public void Init(string url)
        {
            AsyncHelpers.RunSync(async () =>
            {
                _previousOperation = _client.ConnectAsync(new Uri(url), _cts.Token);
                // wait for the establishment of the connection, so we have the correct state.
                await _previousOperation;
            });
        }

        public async Task DisposeAsync()
        {
            try
            {
                if (_cts.IsCancellationRequested == false)
                {
                    await _previousOperation.ConfigureAwait(false);
                    await _client.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "Bulk-Insert", _cts.Token).ConfigureAwait(false);
                    ThrowIfServerStatusFaulted();
                    var ans = await _answerFromServer; // wait for operation to complete.
                    if (ans.CloseStatus == null)
                    {
                        await ThrowAnswerFromServer();
                    }
                }
            }
            finally
            {
                _client.Dispose();
                _jsonWriter?.Dispose();
                _entityWriter?.Dispose();
                _stream?.Dispose();
                _releaseOperationContext?.Dispose();
            }
        }

        public void Dispose()
        {
            AsyncHelpers.RunSync(DisposeAsync);
        }

        public string Store(object entity, string id)
        {
            return Store(entity);
        }

        public string Store(object entity)
        {
            return AsyncHelpers.RunSync(() => StoreAsync(entity));
        }

        public async Task<string> StoreAsync(object entity)
        {
            var id = GetId(entity);
            await StoreAsync(entity, id).ConfigureAwait(false);
            return id;
        }

        public async Task StoreAsync(object entity, string id)
        {
            if (_answerFromServer.IsCompleted)
            {
                await ThrowAnswerFromServer();
            }
            await _previousOperation.ConfigureAwait(false);
            _stream.SetLength(0);
            _jsonWriter.WriteStartObject();
            _jsonWriter.WritePropertyName(id);
            _jsonWriter.Flush();
            _documentConventions.SerializeEntityToJsonStream(entity, _entityWriter);            
            _entityWriter.Flush();
            _jsonWriter.WriteEndObject();
            _jsonWriter.Flush();

            ArraySegment<byte> bytes;
            _stream.TryGetBuffer(out bytes);
            _previousOperation = _client.SendAsync(bytes, WebSocketMessageType.Text, false, _cts.Token);
        }

        private async Task ThrowAnswerFromServer()
        {
            ThrowIfServerStatusFaulted();

            var result = _answerFromServer.Result;
            _recieveBuffer.Valid = result.Count;
            var msg = await _context.ParseToMemoryAsync(
                _client,
                "bulk insert error",
                BlittableJsonDocumentBuilder.UsageMode.None,
                _recieveBuffer);
            string str;
            msg.TryGet("Exception", out str);
            throw new BulkInsertAbortedException(str);
        }

        private void ThrowIfServerStatusFaulted()
        {
            if (_answerFromServer.IsFaulted)
            {
                _cts.Cancel();
                throw new BulkInsertProtocolViolationException("Could not read from server, it status is faulted");
            }
        }

        private string GetId(object entity)
        {
            string id;
            if (_generateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id) == false)
            {
                id = _generateEntityIdOnTheClient.GenerateDocumentKeyForStorage(entity);
                _generateEntityIdOnTheClient.TrySetIdentity(entity, id); //set Id property if it was null
            }
            return id;
        }
    }
}
