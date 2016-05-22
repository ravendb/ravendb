using System;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Platform;
using Raven.Client.Platform.Unix;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ReplicationUtil
{
    public class DocumentReplicationTransport : IDisposable
    {
        private readonly string _url;
        private readonly CancellationToken _cancellationToken;
        private WebSocket _webSocket;
        private bool _disposed;
        private WebsocketStream _websocketStream;

        public DocumentReplicationTransport(string url, CancellationToken cancellationToken)
        {
            _url = url;
            _cancellationToken = cancellationToken;
            _disposed = false;
        }

        private async Task EnsureConnectionAsync()
        {
            if (_webSocket.State != WebSocketState.Open)
            {
                _webSocket = await GetAndConnectWebSocketAsync();
                _websocketStream = new WebsocketStream(_webSocket, _cancellationToken);
            }
        }

        private async Task<WebSocket> GetAndConnectWebSocketAsync()
        {
            var uri = new Uri(_url.Replace("http://", "ws://").Replace(".fiddler", "") + "/");
            if (Sparrow.Platform.Platform.RunningOnPosix)
            {
                var webSocketUnix = new RavenUnixClientWebSocket();
                await webSocketUnix.ConnectAsync(uri, _cancellationToken);

                return webSocketUnix;
            }

            var webSocket = new ClientWebSocket();
            await webSocket.ConnectAsync(uri, _cancellationToken);
            return webSocket;
        }	   

        public async Task SendDocumentBatchAsync(Document[] docs, DocumentsOperationContext context)
        {
            await EnsureConnectionAsync();
            for (int i = 0; i < docs.Length; i++)
            {
                var doc = docs[i];
                context.Write(_websocketStream,doc.Data);				
            }

            await _websocketStream.WriteEndOfMessageAsync();
            await _websocketStream.FlushAsync(_cancellationToken);
        }		

        public async Task SendHeartbeatAsync(DocumentsOperationContext context)
        {
            await EnsureConnectionAsync();
            //TODO : finish heartbeat
        }

        public void Dispose()
        {
            _disposed = true;
            _webSocket.Dispose();
        }				   
    }
}
