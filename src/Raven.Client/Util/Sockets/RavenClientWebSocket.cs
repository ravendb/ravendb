using System;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Platform;

namespace Raven.Client.Util.Sockets
{
    public class RavenClientWebSocket : IDisposable
    {
        private readonly ClientWebSocket _winInstance;

        public RavenClientWebSocket()
        {
            _winInstance = new ClientWebSocket();
        }

        public WebSocketState State
        {
            get
            {
                return _winInstance.State;
            }
        }

        public async Task ConnectAsync(Uri uri, CancellationToken token)
        {
            if (PlatformDetails.RunningOnPosix)
                await _winInstance.ConnectAsync(uri, token);
        }

        public async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> arraySegment, CancellationToken token)
        {
            return await _winInstance.ReceiveAsync(arraySegment, token).ConfigureAwait(false);
        }

        public async Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken token)
        {
                await _winInstance.CloseOutputAsync(closeStatus, statusDescription, token);
        }

        public async Task SendAsync(ArraySegment<byte> segment, WebSocketMessageType messageType, bool endOfMessage, CancellationToken token)
        {
                await _winInstance.SendAsync(segment, messageType, endOfMessage, token);
        }

        public void Dispose()
        {
                _winInstance.Dispose();
        }

        public async Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken token)
        {
                await _winInstance.CloseAsync(closeStatus, statusDescription, token);
        }
    }
}
