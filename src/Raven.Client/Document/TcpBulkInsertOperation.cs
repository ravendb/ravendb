using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Client.Platform;
using Raven.Json.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Document
{
    public class TcpBulkInsertOperation : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(TcpBulkInsertOperation));
        private readonly CancellationTokenSource _cts;
        private readonly Task _getServerResponseTask;
        private UnmanagedBuffersPool _unmanagedBuffersPool;
        private JsonOperationContext _jsonOperationContext;
        private readonly BlockingCollection<MemoryStream> _documents = new BlockingCollection<MemoryStream>();
        private readonly BlockingCollection<MemoryStream> _buffers =
            // we use a stack based back end to ensure that we always use the active buffers
            new BlockingCollection<MemoryStream>(new ConcurrentStack<MemoryStream>());
        private readonly Task _writeToServerTask;
        private DateTime _lastHeartbeat;
        private readonly long _sentAccumulator;

        private readonly ManualResetEventSlim _throttlingEvent = new ManualResetEventSlim();
        private bool _isThrottling;
        private readonly long _maxDiffSizeBeforeThrottling = 20L*1024*1024; // each buffer is 4M. We allow the use of 5-6 buffers out of 8 possible
        private TcpClient _tcpClient;


        ~TcpBulkInsertOperation()
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

        public TcpBulkInsertOperation(AsyncServerClient asyncServerClient, CancellationTokenSource cts)
        {
            _throttlingEvent.Set();
            _unmanagedBuffersPool = new UnmanagedBuffersPool("bulk/insert/client");
            _jsonOperationContext = new JsonOperationContext(_unmanagedBuffersPool);
            _cts = cts ?? new CancellationTokenSource();
            _tcpClient = new TcpClient();

            for (int i = 0; i < 64; i++)
            {
                _buffers.Add(new MemoryStream());
            }


            var connectToServerTask = ConnectToServer(asyncServerClient);
            
            _sentAccumulator = 0;
            _getServerResponseTask = connectToServerTask.ContinueWith(task =>
            {
                ReadServerResponses(task.Result);
            });

            _writeToServerTask = connectToServerTask.ContinueWith(task =>
            {
                WriteToServer(task.Result);
            });

        }

        private async Task<Stream> ConnectToServer(AsyncServerClient asyncServerClient)
        {
            var connectionInfo = await asyncServerClient.GetTcpInfoAsync();
            await _tcpClient.ConnectAsync(new Uri(connectionInfo.Url).Host, connectionInfo.Port);

            _tcpClient.NoDelay = true;
            _tcpClient.SendBufferSize = 32*1024;
            _tcpClient.ReceiveBufferSize = 4096;
            var networkStream = _tcpClient.GetStream();

            var buffer = Encoding.UTF8.GetBytes(new RavenJObject
            {
                ["Database"] = MultiDatabase.GetDatabaseName(asyncServerClient.Url),
                ["Operation"] = "BulkInsert"
            }.ToString());
            await networkStream.WriteAsync(buffer,0, buffer.Length);

            return networkStream;
        }

        private void WriteToServer(Stream serverStream)
        {
            const string debugTag = "bulk/insert/document";
            var jsonParserState = new JsonParserState();
            var buffer = _jsonOperationContext.GetManagedBuffer();
            var streamNetworkBuffer = new MemoryStream();
            using (var jsonParser = new UnmanagedJsonParser(_jsonOperationContext, jsonParserState, debugTag))
            {
                ArraySegment<byte> arraySegment;
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
                        WriteVariableSizeInt(streamNetworkBuffer, builder.SizeInBytes);
                        builder.CopyTo(streamNetworkBuffer);
                    }

                    if (streamNetworkBuffer.Length > 32 * 1024)
                    {
                        streamNetworkBuffer.TryGetBuffer(out arraySegment);
                        serverStream.Write(arraySegment.Array, 0, arraySegment.Count);
                        streamNetworkBuffer.SetLength(0);
                        // first flush, then throttle (in case first buffer to send is big enough to throttle and we rely continuation on server's response)
                        _throttlingEvent.Wait();
                    }
                }

                streamNetworkBuffer.TryGetBuffer(out arraySegment);
                serverStream.Write(arraySegment.Array, 0, arraySegment.Count);
                serverStream.WriteByte(0);// done
            }
        }


        public static void WriteVariableSizeInt(Stream stream, int value)
        {
            // assume that we don't use negative values very often
            var v = (uint)value;
            while (v >= 0x80)
            {
                stream.WriteByte((byte)(v | 0x80));
                v >>= 7;
            }
            stream.WriteByte((byte)v);
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

                var  result = await webSocket.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);

                parser.SetBuffer(buffer.Array, result.Count);
                while (writer.Read() == false)
                {
                    if (result.CloseStatus != null)
                    {
                        // we got incomplete json response.
                        // This might happen if we close the connection but still server sends something
                        if (result.CloseStatus != null)
                            return null;
                    }

                    result = await webSocket.ReceiveAsync(buffer, cancellationToken);

                    parser.SetBuffer(buffer.Array, result.Count);
                }
                writer.FinalizeDocument();
                return writer.CreateReader();
            }
        }

        private void ReadServerResponses(Stream stream)
        {
            bool completed = false;
            using (var context = new JsonOperationContext(_unmanagedBuffersPool))
            {
                do
                {
                    using (var response = context.ReadForMemory(stream, "bulk/insert/message"))
                    {
                        if (response == null)
                        {
                            // we've got disconnection without receiving "Completed" message
                            ReportProgress("Bulk insert aborted because connection with server was disrupted before acknowledging completion");
                            throw new InvalidOperationException("Connection with server was disrupted before acknowledging completion");
                        }

                        string responseType;
                        //TODO: make this strong typed?
                        if (response.TryGet("Type", out responseType))
                        {
                            string msg;
                            switch (responseType)
                            {
                                case "Error":
                                    string exceptionString;
                                    if (response.TryGet("Exception", out exceptionString) == false)
                                        throw new InvalidOperationException("Invalid response from server " +
                                                                            (response.ToString() ?? "null"));
                                    msg =
                                        $"Bulk insert aborted because of server-side exception. Exception information from server : {Environment.NewLine} {exceptionString}";
                                    ReportProgress(msg);
                                    throw new BulkInsertAbortedExeption(msg);

                                case "Processing":
                                    // do nothing. this is hearbeat while server is really busy
                                    break;

                                case "Waiting":
                                    if (_isThrottling)
                                    {
                                        _isThrottling = false;
                                        _throttlingEvent.Set();
                                    }
                                    break;

                                case "Processed":
                                {
                                    long processedSize;
                                    if (response.TryGet("Size", out processedSize) == false)
                                        throw new InvalidOperationException("Invalid Processed response from server " +
                                                                            (response.ToString() ?? "null"));

                                    if (_sentAccumulator - processedSize > _maxDiffSizeBeforeThrottling)
                                    {
                                        if (_isThrottling == false)
                                        {
                                            _throttlingEvent.Reset();
                                            _isThrottling = true;
                                        }
                                    }
                                    else
                                    {
                                        if (_isThrottling)
                                        {
                                            _isThrottling = false;
                                            _throttlingEvent.Set();
                                        }
                                    }
                                }
                                    break;

                                case "Completed":
                                    ReportProgress("Connection closed successfully");
                                    completed = true;
                                    break;
                                default:
                                {
                                    msg = "Received unexpected message from a server : " + responseType;
                                    ReportProgress(msg);
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
                throw new ObjectDisposedException(nameof(TcpBulkInsertOperation));
            }

            metadata[Constants.MetadataDocId] = id;
            data[Constants.Metadata] = metadata;

            var jsonBuffer = _buffers.Take();
            jsonBuffer.SetLength(0);

            data.WriteTo(jsonBuffer);
            jsonBuffer.Position = 0;
            _documents.Add(jsonBuffer);
        }

        public void Dispose()
        {
            DisposeAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public async Task DisposeAsync()
        {
            try
            {
                try
                {
                    _documents.CompleteAdding();
                    await _writeToServerTask.ConfigureAwait(false);

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
                    _tcpClient?.Dispose();
                }
            }
            finally
            {
                _tcpClient = null;
                _jsonOperationContext?.Dispose();
                _jsonOperationContext = null;
                _unmanagedBuffersPool?.Dispose();
                _unmanagedBuffersPool = null;
                GC.SuppressFinalize(this);
            }
        }

        public event Action<string> Report;

        public void Abort()
        {
            throw new NotSupportedException();
            //TODO: implement this
        }

        protected void ReportProgress(string msg)
        {
            Report?.Invoke(msg);
        }
    }
}