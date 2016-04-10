using System;
using System.Diagnostics;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;
using Sparrow.Json;

namespace Raven.Client.Document
{
    public class WebSocketBulkInsertOperation : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof (WebSocketBulkInsertOperation));
        private readonly CancellationTokenSource cts;
        private ClientWebSocket connection;
        private readonly Task socketConnectionTask;
        private readonly MemoryStream _jsonBuffer = new MemoryStream();
        private readonly MemoryStream _networkBuffer = new MemoryStream();
        private readonly BinaryWriter _networkBufferWriter;
        private readonly string url;
        private readonly Task getServerResponseTask;
        private UnmanagedBuffersPool _unmanagedBuffersPool;
        private JsonOperationContext _jsonOperationContext;

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
            this.cts = cts ?? new CancellationTokenSource();
            connection = new ClientWebSocket();
            url = asyncServerClient.Url;

            var serverUri = new Uri(url);
            if (!serverUri.Scheme.Equals("http", StringComparison.OrdinalIgnoreCase) &&
               !serverUri.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Invalid server url scheme, expected only http or https, but got "+ serverUri.Scheme);

            var uriBuilder = new UriBuilder(serverUri)
            {
                Scheme = serverUri.Scheme.Equals("http", StringComparison.OrdinalIgnoreCase) ? "ws" : "wss",
                Path = serverUri.AbsolutePath + "/bulkInsert",
            };

            socketConnectionTask = connection.ConnectAsync(uriBuilder.Uri, this.cts.Token);
            getServerResponseTask = GetServerResponse();
        }

        private async Task GetServerResponse()
        {
            await socketConnectionTask;
            var closeBuffer = new byte[4096];
            WebSocketReceiveResult result;
            string msg;
            do
            {
                result = await connection.ReceiveAsync(new ArraySegment<byte>(closeBuffer), cts.Token);
                if (result.MessageType != WebSocketMessageType.Text)
                    break;
                 msg = Encoding.UTF8.GetString(closeBuffer, 0, result.Count);
            }
            while (msg == "Heartbeat");

            if (result.MessageType != WebSocketMessageType.Close)
            {
                msg = $"Received unexpected message from a server (expected only message about closing, and got message of type == {result.MessageType})";
                ReportProgress(msg);

                try
                {
                    await connection.CloseOutputAsync(WebSocketCloseStatus.ProtocolError, "Aborting bulk-insert because receiving unexpected response from server -> protocol violation", cts.Token)
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
                    await connection.CloseOutputAsync(WebSocketCloseStatus.InternalServerError, "Aborting bulk-insert because of server-side exception", cts.Token)
                        .ConfigureAwait(false);
                }
                catch (Exception )
                {
                    //ignoring any errors here
                }
                throw new BulkInsertAbortedExeption(msg);
            }
            ReportProgress("Connection closed successfully");
        }

        public async Task WriteAsync(string id, RavenJObject metadata, RavenJObject data)
        {
            cts.Token.ThrowIfCancellationRequested();

            await socketConnectionTask.ConfigureAwait(false);

            if (getServerResponseTask.IsFaulted || getServerResponseTask.IsCanceled)
            {
                await getServerResponseTask;
                return;// we should never actually get here, the await will throw
            }

            if (getServerResponseTask.IsCompleted)
            {
                // we can only get here if we closed the connection
                throw new ObjectDisposedException(nameof(WebSocketBulkInsertOperation));
            }

            metadata[Constants.MetadataDocId] = id;
            data[Constants.Metadata] = metadata;

            _jsonBuffer.SetLength(0);

            data.WriteTo(_jsonBuffer);
            _jsonBuffer.Position = 0;
            using (var doc = _jsonOperationContext.Read(_jsonBuffer, id))
            {
                _networkBufferWriter.Write(doc.Size);// TODO: use variable size int
                doc.CopyTo(_networkBuffer);
            }

            if (_networkBuffer.Length > 32*1024)
            {
                await FlushBufferAsync();
            }

        }

        private async Task FlushBufferAsync()
        {
            ArraySegment<byte> segment;
            _networkBuffer.Position = 0;
            _networkBuffer.TryGetBuffer(out segment);

            await connection.SendAsync(segment, WebSocketMessageType.Binary, true, cts.Token)
                .ConfigureAwait(false);
            ReportProgress($"Batch sent to {url} (bytes count = {segment.Count})");

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
                if (connection == null)
                    return;
                try
                {
                    await FlushBufferAsync();

                    await connection.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "Finished bulk-insert", cts.Token)
                        .ConfigureAwait(false);

                    //Make sure that if the server goes down 
                    //in the last moment, we do not get stuck here.
                    //In general, 1 minute should be more than enough for the 
                    //server to finish its stuff and get back to client
                    var timeoutTask = Task.Delay(Debugger.IsAttached ? TimeSpan.FromMinutes(30) : TimeSpan.FromSeconds(30));
                    var res = await Task.WhenAny(timeoutTask, getServerResponseTask);
                    if(timeoutTask == res)
                        throw new TimeoutException("Wait for bulk-insert closing message from server, but it didn't happen. Maybe the server went down (most likely) and maybe this is due to a bug. In any case,this needs to be investigated.");
                }
                catch (Exception e)
                {
                    if (e is TimeoutException)
                        throw;

                    // those can throw, but we are shutting down anyway, so no point in 
                    // doing anything here
                }
                finally
                {
                    connection.Dispose();
                }
            }
            finally
            {
                connection = null;
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
            ReportProgress($"Bulk-insert to {url} aborted");
            cts.Cancel();
        }

        protected void ReportProgress(string msg)
        {
            Report?.Invoke(msg);
        }
    }
}