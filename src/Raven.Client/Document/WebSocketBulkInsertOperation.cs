using System;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
    public class WebSocketBulkInsertOperation 
    {
        private readonly CancellationTokenSource cts;
        private ClientWebSocket connection;
        private readonly Task socketConnectionTask;
        private readonly MemoryStream buffer = new MemoryStream();
        private readonly string url;
        private readonly Task getServerResponseTask;


        public WebSocketBulkInsertOperation(AsyncServerClient asyncServerClient, CancellationTokenSource cts)
        {
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
            var result = await connection.ReceiveAsync(new ArraySegment<byte>(closeBuffer), cts.Token);
            if (result.MessageType != WebSocketMessageType.Close)
            {
                var msg = $"Received unexpected message from a server (expected only message about closing, and got message of type == {result.MessageType})";
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
                var msg =
                    $"Bulk insert aborted because of server-side exception. Exception information from server : {Environment.NewLine} {exceptionString}";
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

            buffer.SetLength(0);
            data.WriteTo(buffer);

            ArraySegment<byte> segment;
            buffer.Position = 0;
            buffer.TryGetBuffer(out segment);

            await connection.SendAsync(segment, WebSocketMessageType.Text, false, cts.Token)
                            .ConfigureAwait(false);

            ReportProgress($"document {id} sent to {url} (bytes count = {segment.Count})");
        }


        public void Dispose()
        {
            AsyncHelpers.RunSync(DisposeAsync);
        }

        public async Task DisposeAsync()
        {
            if (connection == null)
                return;

            try
            {
                try
                {
                    await connection.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "Finished bulk-insert", cts.Token)
                        .ConfigureAwait(false);
                    await getServerResponseTask;
                }
                catch (Exception )
                {
                    // those can throw, but we are shutting down anyway, so no point in 
                    // doing anything here
                }
                connection.Dispose();
            }
            finally
            {
                connection = null;
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