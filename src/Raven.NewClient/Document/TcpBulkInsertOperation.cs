using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.NewClient.Abstractions;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Logging;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Platform;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using AsyncHelpers = Raven.NewClient.Abstractions.Util.AsyncHelpers;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Exceptions.BulkInsert;
using Raven.NewClient.Client.Exceptions.Security;
using Raven.NewClient.Client.Json;

namespace Raven.NewClient.Client.Document
{
    public class TcpBulkInsertOperation : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(TcpBulkInsertOperation));
        private readonly CancellationTokenSource _cts;
        private readonly Task _getServerResponseTask;
        private readonly BlockingCollection<Tuple<object,string>> _documents = new BlockingCollection<Tuple<object, string>>();
        private readonly IDocumentStore _store;
        private readonly EntityToBlittable _entityToBlittable;
        private readonly Task _writeToServerTask;
        private DateTime _lastHeartbeat;
        private readonly long _sentAccumulator;
        private readonly ManualResetEventSlim _throttlingEvent = new ManualResetEventSlim();
        private bool _isThrottling;
        private readonly long _maxDiffSizeBeforeThrottling = 20L * 1024 * 1024; // each buffer is 4M. We allow the use of 5-6 buffers out of 8 possible
        private TcpClient _tcpClient;
        private string _url;
        private JsonContextPool _contextPool;

        private readonly ManualResetEventSlim _headerResponseFinished = new ManualResetEventSlim();

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

        public TcpBulkInsertOperation(string database, IDocumentStore store, RequestExecuter requestExecuter, CancellationTokenSource cts)
        {
            _throttlingEvent.Set();
            _cts = cts ?? new CancellationTokenSource();
            _tcpClient = new TcpClient();

            _store = store;
            _contextPool = store.GetRequestExecuter(database).ContextPool;
            _entityToBlittable = new EntityToBlittable(null);
            var connectToServerTask = ConnectToServer(requestExecuter);

            _sentAccumulator = 0;
            _getServerResponseTask = connectToServerTask.ContinueWith(task =>
            {
                ReadServerResponses(task.Result);
            });

            _writeToServerTask = connectToServerTask.ContinueWith(task =>
            {
                WriteToServer(database, task.Result);
            });
        }

        private async Task<ConnectToServerResult> ConnectToServer(RequestExecuter requestExecuter)
        {
            var command = new GetTcpInfoCommand();
            JsonOperationContext context;
            string apiToken;
            using (requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                await requestExecuter.ExecuteAsync(command, context).ConfigureAwait(false);
                apiToken = await requestExecuter.GetAuthenticationToken(context, command.RequestedNode);
            }


            var uri = new Uri(command.Result.Url);
            await _tcpClient.ConnectAsync(uri.Host, uri.Port).ConfigureAwait(false);

            _tcpClient.NoDelay = true;
            _tcpClient.SendBufferSize = 32 * 1024;
            _tcpClient.ReceiveBufferSize = 4096;
            var networkStream = _tcpClient.GetStream();

            return new ConnectToServerResult {OAuthToken = apiToken, Stream = networkStream};
        }

        private void WriteToServer(string database, ConnectToServerResult connection)
        {
            var header = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new TcpConnectionHeaderMessage
            {
                DatabaseName = database,
                Operation = TcpConnectionHeaderMessage.OperationTypes.BulkInsert,
                AuthorizationToken = connection.OAuthToken
            }));
            connection.Stream.Write(header, 0, header.Length);
            connection.Stream.Flush();
            
            JsonOperationContext context;
            //Reading reply from server
            using (_contextPool.AllocateOperationContext(out context))
            using (var response = context.ReadForMemory(connection.Stream, "bulkinserttcp-header-response")) 
            {
                
                var reply = JsonDeserializationClient.TcpConnectionHeaderResponse(response);
                switch (reply.Status)
                {
                    case TcpConnectionHeaderResponse.AuthorizationStatus.Forbidden:
                        throw AuthorizationException.Forbidden(database);
                    case TcpConnectionHeaderResponse.AuthorizationStatus.Success:
                        break;
                    default:
                        throw AuthorizationException.Unauthorized(reply.Status, database);
                }
            }

            _headerResponseFinished.Set();

            while (_documents.IsCompleted == false)
            {
                _cts.Token.ThrowIfCancellationRequested();
                Tuple<object,string> doc;
                try
                {
                    doc = _documents.Take();
                }
                catch (InvalidOperationException)
                {
                    break;
                }
                var needToThrottle = _throttlingEvent.Wait(0) == false;
                                
                using (_contextPool.AllocateOperationContext(out context))
                {
                    JsonOperationContext.ManagedPinnedBuffer pinnedBuffer;
                    using (context.GetManagedBuffer(out pinnedBuffer))
                    {
                        var documentInfo = new DocumentInfo();

                        var metadata = new DynamicJsonValue();
                        var tag = _store.Conventions.GetDynamicTagName(doc.Item1);
                        if (tag != null)
                            metadata[Constants.Metadata.Collection] = tag;
                        metadata[Constants.Metadata.Id] = doc.Item2;

                        documentInfo.Metadata = context.ReadObject(metadata, doc.Item2);
                        var data = _entityToBlittable.ConvertEntityToBlittable(doc.Item1, _store.Conventions, context, documentInfo);
                        WriteVariableSizeInt(connection.Stream, data.Size);
                        WriteToStream(connection.Stream, data, pinnedBuffer);

                        if (needToThrottle)
                        {
                            connection.Stream.Flush();
                            _throttlingEvent.Wait(500);
                        }
                    }
                }
            }
            connection.Stream.WriteByte(0); //done
            connection.Stream.Flush();
        }

        private static unsafe void WriteToStream(Stream networkBufferedStream, BlittableJsonReaderObject reader, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            var bytes = reader.BasePointer;
            var pBuffer = buffer.Pointer;
            var remainingSize = reader.Size;
            while (remainingSize > 0)
            {
                var size = Math.Min(remainingSize, buffer.Length);
                Memory.Copy(pBuffer, bytes + (reader.Size - remainingSize), size);
                remainingSize -= size;
                networkBufferedStream.Write(buffer.Buffer.Array, buffer.Buffer.Offset, size);
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
            JsonOperationContext.ManagedPinnedBuffer bytes;
            using (context.GetManagedBuffer(out bytes))
            using (var parser = new UnmanagedJsonParser(context, jsonParserState, debugTag))
            {
                var writer = new BlittableJsonDocumentBuilder(context,
                    BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, jsonParserState);

                writer.ReadObjectDocument();

                var result = await webSocket.ReceiveAsync(bytes.Buffer, cancellationToken).ConfigureAwait(false);

                parser.SetBuffer(bytes,0, result.Count);
                while (writer.Read() == false)
                {
                    // we got incomplete json response.
                    // This might happen if we close the connection but still server sends something
                    if (result.CloseStatus != null)
                        return null;

                    result = await webSocket.ReceiveAsync(bytes.Buffer, cancellationToken).ConfigureAwait(false);

                    parser.SetBuffer(bytes, 0, result.Count);
                }
                writer.FinalizeDocument();
                return writer.CreateReader();
            }
        }

        private void ReadServerResponses(ConnectToServerResult connection)
        {
            _headerResponseFinished.Wait(_cts.Token);
            _headerResponseFinished.Dispose();
            Stream stream = connection.Stream;
            bool completed = false;
            using (var context = new JsonOperationContext(4096, 1024))
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
                                    throw new BulkInsertAbortedException(msg);

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
                                        throw new BulkInsertProtocolViolationException(msg);
                                    }
                            }
                        }
                        _lastHeartbeat = SystemTime.UtcNow;
                    }
                } while (completed == false);
            }
        }

        public async Task WriteAsync(string id, object data)
        {
            _cts.Token.ThrowIfCancellationRequested();

            await AssertValidServerConnection().ConfigureAwait(false);// we should never actually get here, the await will throw

            _documents.Add(new Tuple<object, string>(data,id));

        }

        private async Task AssertValidServerConnection()
        {
            if (_getServerResponseTask.IsFaulted || _getServerResponseTask.IsCanceled)
                await _getServerResponseTask.ConfigureAwait(false);

            if (_writeToServerTask.IsFaulted || _writeToServerTask.IsCanceled)
                await _getServerResponseTask.ConfigureAwait(false);

            if (_getServerResponseTask.IsCompleted)
                // we can only get here if we closed the connection
                throw new ObjectDisposedException(nameof(TcpBulkInsertOperation));
        }

        public void Dispose()
        {
            AsyncHelpers.RunSync(DisposeAsync);
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
                        var res = await Task.WhenAny(timeoutTask, _getServerResponseTask).ConfigureAwait(false);
                        if (timeoutTask != res)
                            break;
                        if (SystemTime.UtcNow - _lastHeartbeat > timeDelay + TimeSpan.FromSeconds(60))
                        {
                            // Waited to much for close msg from server in bulk insert.. can happen in huge bulk insert ratios.
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