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
using Sparrow;
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
        private long _sentAccomulator;

        private readonly AsyncManualResetEvent _throttlingEvent = new AsyncManualResetEvent();
        private bool _isThrottling;
        private readonly long _maxDiffSizeBeforeThrottling = 20L*1024*1024; // each buffer is 4M. We allow the use of 5-6 buffers out of 8 possible

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
            _throttlingEvent.Set();
            _unmanagedBuffersPool = new UnmanagedBuffersPool("bulk/insert/client");
            _jsonOperationContext = new JsonOperationContext(_unmanagedBuffersPool);
            _networkBufferWriter = new BinaryWriter(_networkBuffer);
            _cts = cts ?? new CancellationTokenSource();
            _connection = new RavenClientWebSocket();
            _url = asyncServerClient.Url;

            var serverUri = new Uri(_url);
            if (!serverUri.Scheme.Equals("http", StringComparison.OrdinalIgnoreCase) &&
               !serverUri.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Invalid server url scheme, expected only http or https, but got " + serverUri.Scheme);

            var uriBuilder = new UriBuilder(serverUri)
            {
                Scheme = serverUri.Scheme.Equals("http", StringComparison.OrdinalIgnoreCase) ? "ws" : "wss",
                Path = serverUri.AbsolutePath + "/bulkInsert"
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
            const string debugTag = "bulk/insert/document";
            var jsonParserState = new JsonParserState();
            var buffer = _jsonOperationContext.GetManagedBuffer();
            using (var jsonParser = new UnmanagedJsonParser(_jsonOperationContext, jsonParserState, debugTag))
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
                        BlittableJsonDocumentBuilder.UsageMode.ToDisk, debugTag,
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

                        // first flush, then throttle (in case first buffer to send is big enough to throttle and we rely continuation on server's response)
                        await _throttlingEvent.WaitAsync().ConfigureAwait(false);
                    }
                }

                await FlushBufferAsync();
            }
            return 0;
        }


        public async Task<BlittableJsonReaderObject> TryReadFromWebSocket(
           JsonOperationContext context,
            RavenClientWebSocket webSocket,
           string debugTag,
           CancellationToken cancellationToken)
        {
            var jsonParserState = new JsonParserState();
            using (var parser = new UnmanagedJsonParser(context, jsonParserState, debugTag))
            {
                var buffer = new ArraySegment<byte>(context.GetManagedBuffer());

                var writer = new BlittableJsonDocumentBuilder(context,
                    BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, jsonParserState);

                writer.ReadObject();
                var result = await webSocket.ReceiveAsync(buffer, cancellationToken);

                parser.SetBuffer(buffer.Array, result.Count);
                while (writer.Read() == false)
                {
                    if (result.CloseStatus != null)
                    {
                        // we got incomplete json response.
                        // This might happen if we close the connection but still server sends something
                        if (result.CloseStatus != null)
                        {
                            Console.WriteLine("ERRRR :::: " + result.CloseStatus.Value + " , " + result.MessageType +
                                              " , " + result.CloseStatusDescription + "<<" +
                                              Encoding.UTF8.GetString(buffer.Array, 0, result.Count) + ">>");
                            return null;
                        }
                    }

                    result = await webSocket.ReceiveAsync(buffer, cancellationToken);
                    parser.SetBuffer(buffer.Array, result.Count);
                }
                writer.FinalizeDocument();
                return writer.CreateReader();
            }
        }

        private async Task GetServerResponse()
        {
            await _socketConnectionTask;
            string msg;

            bool completed = false;
            using (var context = new JsonOperationContext(_unmanagedBuffersPool))
            {
                do
                {
                    using (var response =
                        await TryReadFromWebSocket(context, _connection, "Bulk/Insert/GetServerResponse", _cts.Token))
                    {
                        if (response == null)
                        {
                            // we've got disconnection without receiving "Completed" message
                            ReportProgress("Bulk insert aborted because connection with server was disrupted before acknowledging completion");
                            throw new InvalidOperationException("Connection with server was disrupted before acknowledging completion");
                        }

                        string responseType;
                        if (response.TryGet("Type", out responseType))
                        {
                            switch (responseType)
                            {
                                case "Error":
                                    {
                                        string exceptionString;
                                        if (response.TryGet("Exception", out exceptionString) == false)
                                            throw new InvalidOperationException("Invalid response from server " +
                                                                                (response.ToString() ?? "null"));
                                        msg =
                                            $"Bulk insert aborted because of server-side exception. Exception information from server : {Environment.NewLine} {exceptionString}";
                                        ReportProgress(msg);
                                        await
                                            SendCloseMessage(WebSocketCloseStatus.InternalServerError,
                                                "Aborting bulk-insert because of server-side exception");
                                    }
                                    throw new BulkInsertAbortedExeption(msg);

                                case "Processing":
                                    // do nothing. this is hearbeat while server is really busy
                                break;

                                case "Waiting":
                                    Console.WriteLine("WAITING RECVED");

                                    _isThrottling = false;
                                    _throttlingEvent.Set();
                                    break;

                                case "Processed":
                                    {
                                        long processedSize;
                                        if (response.TryGet("Size", out processedSize) == false)
                                            throw new InvalidOperationException("Invalid Processed response from server " +
                                                                                (response.ToString() ?? "null"));

                                        //Console.WriteLine("Info: " + _sentAccomulator + " - " + processedSize + " = " +
                                        //                  (_sentAccomulator - processedSize >
                                        //                   _maxDiffSizeBeforeThrottling));

                                        if (_sentAccomulator - processedSize > _maxDiffSizeBeforeThrottling)
                                        {
                                            if (_isThrottling == false)
                                            {
                                                _isThrottling = true;
                                                _throttlingEvent.Reset();
                                                Console.WriteLine("Throttle ON");
                                            }
                                        }
                                        else
                                        {
                                            if (_isThrottling)
                                            {
                                                _isThrottling = false;
                                                _throttlingEvent.Set();
                                                Console.WriteLine("Throttle OFF");
                                            }
                                        }
                                    }
                                    break;

                                case "Completed":
                                    {
                                        var buffer = new ArraySegment<byte>(context.GetManagedBuffer());
                                        var result = await _connection.ReceiveAsync(buffer, _cts.Token);
                                        if (result.MessageType != WebSocketMessageType.Close)
                                        {
                                            msg =
                                                $"Received unexpected message from a server (expected only message about closing, and got message of type == {result.MessageType})";
                                            ReportProgress(msg);
                                            await
                                                SendCloseMessage(WebSocketCloseStatus.ProtocolError,
                                                    "Aborting bulk-insert because receiving unexpected response from server -> protocol violation");
                                            throw new BulkInsertProtocolViolationExeption(msg);
                                        }
                                    }
                                    ReportProgress("Connection closed successfully");
                                    completed = true;
                                    break;
                                default:
                                    {
                                        msg = "Received unexpected message from a server : " + responseType;
                                        ReportProgress(msg);
                                        await
                                            SendCloseMessage(WebSocketCloseStatus.ProtocolError,
                                                "Aborting bulk-insert because receiving unexpected response from server -> protocol violation");
                                        throw new BulkInsertProtocolViolationExeption(msg);
                                    }
                            }
                        }
                        _lastHeartbeat = SystemTime.UtcNow;
                    }
                } while (completed == false);
            }
        }

        public async Task WriteAsync(string id, RavenJObject metadata, RavenJObject data)
        {
            _cts.Token.ThrowIfCancellationRequested();

            await _socketConnectionTask.ConfigureAwait(false);

            if (_getServerResponseTask.IsFaulted || _getServerResponseTask.IsCanceled)
            {
                await _getServerResponseTask.ConfigureAwait(false);
                return;// we should never actually get here, the await will throw
            }

            if (_writeToServerTask.IsFaulted || _writeToServerTask.IsCanceled)
            {
                await _getServerResponseTask.ConfigureAwait(false);
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

            _sentAccomulator += _networkBuffer.Length;

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
                        await _writeToServerTask.ConfigureAwait(false);
                    }
                    catch
                    {
                        await SendCloseMessage(WebSocketCloseStatus.InternalServerError, "Error sending documents").ConfigureAwait(false);
                        throw;
                    }
                    await SendCloseMessage(WebSocketCloseStatus.InternalServerError, "Finished bulk-insert").ConfigureAwait(false);

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
                        if (SystemTime.UtcNow - _lastHeartbeat > timeDelay + TimeSpan.FromSeconds(60))
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


        private async Task SendCloseMessage(WebSocketCloseStatus msgType, string closeMessage)
        {
            if (_connection.State != WebSocketState.Open)
                return;
            try
            {
                await _connection.CloseOutputAsync(msgType,
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