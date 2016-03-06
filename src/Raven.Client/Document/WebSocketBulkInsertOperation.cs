using System;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
    public class WebSocketBulkInsertOperation : IBulkInsertOperation
    {
        private readonly BulkInsertOptions options;
        private readonly IDatabaseChanges changes;
        private readonly CancellationTokenSource cts;
        private ClientWebSocket connection;
        private readonly Task socketConnectionTask;
        private readonly MemoryStream buffer = new MemoryStream();
        private readonly string url;

        public Guid OperationId { get; }
        public bool IsAborted { get; }
        
        public WebSocketBulkInsertOperation(BulkInsertOptions options,
            AsyncServerClient asyncServerClient, 
            IDatabaseChanges changes,CancellationTokenSource cts)
        {
            this.options = options;
            this.changes = changes;
            this.cts = cts ?? new CancellationTokenSource();
            connection = new ClientWebSocket();
            url = asyncServerClient.Url;
            var hostname = $"{url.Replace("http://", "ws://").Replace(".fiddler", "")}/bulkInsert";
            socketConnectionTask = connection.ConnectAsync(new Uri(hostname), this.cts.Token);
        }		


        public async Task WriteAsync(string id, RavenJObject metadata, RavenJObject data)
        {
            await EnsureConnection().ConfigureAwait(false);
            metadata[Constants.MetadataDocId] = id;
            data[Constants.Metadata] = metadata;

            buffer.SetLength(0);
            data.WriteTo(buffer);

            ArraySegment<byte> segment;
            buffer.Position = 0;
            buffer.TryGetBuffer(out segment);

            await connection.SendAsync(segment, WebSocketMessageType.Text, false, cts.Token)
                            .ConfigureAwait(false);

            buffer.Position = 0;
            await ReceiveResponseAndThrowIfError(segment);

            ReportProgress($"Bulk-insert -> document sent to {url} (bytes count = {segment.Count})");
        }

        private async Task ReceiveResponseAndThrowIfError(ArraySegment<byte> segment)
        {
            await connection.ReceiveAsync(segment, cts.Token);
            if(segment.Array[0] != 0)
                throw new InvalidOperationException("Bulk-insert has thrown a server-side error. Check server logs for more details");
        }

        public void Dispose()
        {
            AsyncHelpers.RunSync(DisposeAsync);
        }

        public async Task<int> DisposeAsync()
        {
            try
            {
                if (connection != null && connection.State != WebSocketState.Closed)
                {
                    await connection.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "Finished bulk-insert", cts.Token)
                                    .ConfigureAwait(false);
                }
                connection?.Dispose();
                return connection == null ? 1 : 0;
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
        

        private async Task EnsureConnection()
        {
            await socketConnectionTask.ConfigureAwait(false);
            if(connection == null)
                throw new ObjectDisposedException("WebSocketBulkInsertOperation is not usable after being disposed..");

            //TODO: make timeout configurable through bulkinsert options
            if (connection.State != WebSocketState.Open && 
                !SpinWait.SpinUntil(() => connection.State == WebSocketState.Open, TimeSpan.FromSeconds(10)))
                throw new TimeoutException("Timed-out trying to connect to bulk-insert endpoint");

            ReportProgress($"Bulk-insert to {url} connected");
        }

        protected virtual void ReportProgress(string msg)
        {
            Report?.Invoke(msg);
        }
    }
}