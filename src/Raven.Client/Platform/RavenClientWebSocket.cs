using System;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Platform.Unix;
using Sparrow.Platform;

// TODO: Move to Raven.NewClient.Client.Http
namespace Raven.Client.Platform
{
    public class RavenClientWebSocket : IDisposable
    {
        private readonly RavenUnixClientWebSocket _unixInstance;
        private readonly ClientWebSocket _winInstance;

        public RavenClientWebSocket()
        {
            if (PlatformDetails.RunningOnPosix)
                _unixInstance = new RavenUnixClientWebSocket();
            else
                _winInstance = new ClientWebSocket();
        }

        public WebSocketState State
        {
            get
            {
                if (PlatformDetails.RunningOnPosix)
                    return _unixInstance.State;
                return _winInstance.State;
            }
        }

        public async Task ConnectAsync(Uri uri, CancellationToken token)
        {
            if (PlatformDetails.RunningOnPosix)
                await _unixInstance.ConnectAsync(uri, token);
            else
                await _winInstance.ConnectAsync(uri, token);
        }

        public async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> arraySegment, CancellationToken token)
        {
            if (PlatformDetails.RunningOnPosix)
                return await _unixInstance.ReceiveAsync(arraySegment, token);
            return await _winInstance.ReceiveAsync(arraySegment, token).ConfigureAwait(false);
        }

        public async Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken token)
        {
            if (PlatformDetails.RunningOnPosix)
                await _unixInstance.CloseOutputAsync(closeStatus, statusDescription, token);
            else
                await _winInstance.CloseOutputAsync(closeStatus, statusDescription, token);
        }

        public async Task SendAsync(ArraySegment<byte> segment, WebSocketMessageType messageType, bool endOfMessage, CancellationToken token)
        {
            if (PlatformDetails.RunningOnPosix)
                await _unixInstance.SendAsync(segment, messageType, endOfMessage, token);
            else
                await _winInstance.SendAsync(segment, messageType, endOfMessage, token);
        }

        public void Dispose()
        {
            if (PlatformDetails.RunningOnPosix)
                _unixInstance.Dispose();
            else
                _winInstance.Dispose();
        }

        public async Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken token)
        {
            if (PlatformDetails.RunningOnPosix)
                await _unixInstance.CloseAsync(closeStatus, statusDescription, token);
            else
                await _winInstance.CloseAsync(closeStatus, statusDescription, token);
        }
    }
}
