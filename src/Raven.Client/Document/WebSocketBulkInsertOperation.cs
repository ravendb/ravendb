using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Client.Connection.Async;
using Raven.Client.Platform;
using Raven.Json.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Document
{
    public class WebSocketBulkInsertOperation : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(WebSocketBulkInsertOperation));
        private readonly CancellationTokenSource _cts;
        private RavenClientWebSocket _connection;
        private readonly Task _socketConnectionTask;
        private readonly MemoryStream _networkBuffer = new MemoryStream();
        private readonly BinaryWriter _networkBufferWriter;
        private readonly string _url;
        private readonly Task _getServerResponseTask;
        private UnmanagedBuffersPool _unmanagedBuffersPool;
        private JsonOperationContext _jsonOperationContext;
        private readonly BlockingCollection<MemoryStream> _documents = new BlockingCollection<MemoryStream>();
        private readonly BlockingCollection<MemoryStream> _buffers =
            // we use a stack based back end to ensure that we always use the active buffers
            new BlockingCollection<MemoryStream>(new ConcurrentStack<MemoryStream>());
        private readonly Task _writeToServerTask;
        private DateTime _lastHeartbeat;


        ~WebSocketBulkInsertOperation()
        {
            try
            {
                Log.Warn("Web socket bulk insert was not disposed, and is cleaned via finalizer");
                Dispose();
            }
            catch (Exception e)
            {
                Log.WarnException("Failed to dispose web socket bulk operation from finalizer", e);
            }
        }

        public WebSocketBulkInsertOperation(AsyncServerClient asyncServerClient, CancellationTokenSource cts)
        {
            _unmanagedBuffersPool = new UnmanagedBuffersPool("bulk/insert/client");
            _jsonOperationContext = new JsonOperationContext(_unmanagedBuffersPool);
            _networkBufferWriter = new BinaryWriter(_networkBuffer);
            this._cts = cts ?? new CancellationTokenSource();
            _connection = new RavenClientWebSocket();
            _url = asyncServerClient.Url;

            var serverUri = new Uri(_url);
            if (!serverUri.Scheme.Equals("http", StringComparison.OrdinalIgnoreCase) &&
               !serverUri.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Invalid server url scheme, expected only http or https, but got " + serverUri.Scheme);

            var uriBuilder = new UriBuilder(serverUri)
            {
                Scheme = serverUri.Scheme.Equals("http", StringComparison.OrdinalIgnoreCase) ? "ws" : "wss",
                Path = serverUri.AbsolutePath + "/bulkInsert",
            };

            for (int i = 0; i < 64; i++)
            {
                _buffers.Add(new MemoryStream());
            }
            
            _socketConnectionTask = _connection.ConnectAsync(uriBuilder.Uri, this._cts.Token);
            _getServerResponseTask = GetServerResponse();
            _writeToServerTask = Task.Run(async () => await WriteToServer().ConfigureAwait(false));
            
        }

        private async Task<int> WriteToServer()
        {
            
            const string DebugTag = "bulk/insert/document";
            var jsonParserState = new JsonParserState();
            var buffer = _jsonOperationContext.GetManagedBuffer();
            using (var jsonParser = new UnmanagedJsonParser(_jsonOperationContext, jsonParserState, DebugTag))
            {
                
                while (_documents.IsCompleted == false)
                {
                    _cts.Token.ThrowIfCancellationRequested();
                    
                    MemoryStream jsonBuffer;

                    try
                    {
                        jsonBuffer = _documents.Take();
                    }
                    catch (InvalidOperationException)
                    {
                        break;
                    }
                    
                    using (var builder = new BlittableJsonDocumentBuilder(_jsonOperationContext,
                        BlittableJsonDocumentBuilder.UsageMode.ToDisk, DebugTag,
                        jsonParser, jsonParserState))
                    {

                        _jsonOperationContext.CachedProperties.NewDocument();
                        builder.ReadObject();
                        while (true)
                        {
                            var read = jsonBuffer.Read(buffer, 0, buffer.Length);
                            if (read == 0)
                                throw new EndOfStreamException("Stream ended without reaching end of json content");
                            jsonParser.SetBuffer(buffer, read);
                            if (builder.Read())
                                break;
                        }
                        _buffers.Add(jsonBuffer);
                        builder.FinalizeDocument();
                        _networkBufferWriter.Write(builder.SizeInBytes); //TODO: variable length int?
                        builder.CopyTo(_networkBuffer);

                    }
                    
                    if (_networkBuffer.Length > 32 * 1024)
                    {
                        await FlushBufferAsync();
                    }
                }
                
                await FlushBufferAsync();
            }
            return 0;
        }

        private async Task GetServerResponse()
        {
            await _socketConnectionTask;
            var closeBuffer = new byte[4096];
            WebSocketReceiveResult result;
            string msg;
            do
            {
                result = await _connection.ReceiveAsync(new ArraySegment<byte>(closeBuffer), _cts.Token);
                if (result.MessageType != WebSocketMessageType.Text)
                    break;
                msg = Encoding.UTF8.GetString(closeBuffer, 0, result.Count);
                _lastHeartbeat = SystemTime.UtcNow;
            }
            while (msg == "Heartbeat");

            if (result.MessageType != WebSocketMessageType.Close)
            {
                msg = $"Received unexpected message from a server (expected only message about closing, and got message of type == {result.MessageType})";
                ReportProgress(msg);

                try
                {
                    await _connection.CloseOutputAsync(WebSocketCloseStatus.ProtocolError, "Aborting bulk-insert because receiving unexpected response from server -> protocol violation", _cts.Token)
                        .ConfigureAwait(false);
                }
                catch (Exception)
                {
                    //ignoring any errors here
                }
                throw new BulkInsertProtocolViolationExeption(msg);
            }
            if (result.CloseStatus == WebSocketCloseStatus.InternalServerError)
            {
                var exceptionString = Encoding.UTF8.GetString(closeBuffer);
                msg = $"Bulk insert aborted because of server-side exception. Exception information from server : {Environment.NewLine} {exceptionString}";
                ReportProgress(msg);
                try
                {
                    await _connection.CloseOutputAsync(WebSocketCloseStatus.InternalServerError, "Aborting bulk-insert because of server-side exception", _cts.Token)
                        .ConfigureAwait(false);
                }
                catch (Exception)
                {
                    //ignoring any errors here
                }
                throw new BulkInsertAbortedExeption(msg);
            }
            ReportProgress("Connection closed successfully");
        }

        public async Task WriteAsync(string id, RavenJObject metadata, RavenJObject data)
        {
            _cts.Token.ThrowIfCancellationRequested();
            
            await _socketConnectionTask.ConfigureAwait(false);
            
            if (_getServerResponseTask.IsFaulted || _getServerResponseTask.IsCanceled)
            {
                await _getServerResponseTask;
                return;// we should never actually get here, the await will throw
            }

            if (_writeToServerTask.IsFaulted || _writeToServerTask.IsCanceled)
            {
                await _getServerResponseTask;
                return;// we should never actually get here, the await will throw
            }

            if (_getServerResponseTask.IsCompleted)
            {
                // we can only get here if we closed the connection
                throw new ObjectDisposedException(nameof(WebSocketBulkInsertOperation));
            }
           
            metadata[Constants.MetadataDocId] = id;
            data[Constants.Metadata] = metadata;

            var jsonBuffer = _buffers.Take();
            jsonBuffer.SetLength(0);

            data.WriteTo(jsonBuffer);
            jsonBuffer.Position = 0;
            _documents.Add(jsonBuffer);
        }

        private async Task FlushBufferAsync()
        {
            ArraySegment<byte> segment;
            _networkBuffer.Position = 0;
            _networkBuffer.TryGetBuffer(out segment);

            await _socketConnectionTask.ConfigureAwait(false);

            await _connection.SendAsync(segment, WebSocketMessageType.Binary, true, _cts.Token)
                .ConfigureAwait(false);
            ReportProgress($"Batch sent to {_url} (bytes count = {segment.Count})");

            _networkBuffer.SetLength(0);
        }


        public void Dispose()
        {
            DisposeAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public async Task DisposeAsync()
        {
            try
            {
                if (_connection == null)
                    return;
                try
                {
                    _documents.CompleteAdding();
                    try
                    {
                        await _writeToServerTask;
                    }
                    catch
                    {
                        await SendCloseMessage("Error sending documents").ConfigureAwait(false);
                        throw;
                    }
                    await SendCloseMessage("Finished bulk-insert").ConfigureAwait(false);

                    //Make sure that if the server goes down 
                    //in the last moment, we do not get stuck here.
                    //In general, 1 minute should be more than enough for the 
                    //server to finish its stuff and get back to client

                    var timeDelay = Debugger.IsAttached ? TimeSpan.FromMinutes(30) : TimeSpan.FromSeconds(30);
                    while (true)
                    {
                        var timeoutTask = Task.Delay(timeDelay);
                        var res = await Task.WhenAny(timeoutTask, _getServerResponseTask);
                        if (timeoutTask != res)
                            break;
                        if (SystemTime.UtcNow - _lastHeartbeat > timeDelay + TimeSpan.FromSeconds(30))
                        {
                            // TODO: Waited to much for close msg from server in bulk insert.. can happen in huge bulk insert ratios.
                            throw new TimeoutException("Wait for bulk-insert closing message from server, but it didn't happen. Maybe the server went down (most likely) and maybe this is due to a bug. In any case,this needs to be investigated.");
                        }
                    }
                }
                finally
                {
                    _connection.Dispose();
                }
            }
            finally
            {
                _connection = null;
                _jsonOperationContext?.Dispose();
                _jsonOperationContext = null;
                _unmanagedBuffersPool?.Dispose();
                _unmanagedBuffersPool = null;
                GC.SuppressFinalize(this);
            }
        }

        private async Task SendCloseMessage(string closeMessage)
        {
            if (_connection.State != WebSocketState.Open)
                return;
            try
            {
                await _connection.CloseOutputAsync(WebSocketCloseStatus.InternalServerError,
                    closeMessage,
                    _cts.Token)
                    .ConfigureAwait(false);
            }
            catch (Exception)
            {
                // ignoring this error
            }
        }

        public event Action<string> Report;

        public void Abort()
        {
            ReportProgress($"Bulk-insert to {_url} aborted");
            _cts.Cancel();
        }

        protected void ReportProgress(string msg)
        {
            Report?.Invoke(msg);
        }
    }
}