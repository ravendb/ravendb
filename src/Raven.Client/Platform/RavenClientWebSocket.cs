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
        private readonly RavenUnixClientWebSocket unixInstance;
        private readonly ClientWebSocket winInstance;

        public RavenClientWebSocket()
        {
            if (PlatformDetails.RunningOnPosix)
                unixInstance = new RavenUnixClientWebSocket();
            else
                winInstance = new ClientWebSocket();
        }

        public WebSocketState State
        {
            get
            {
                if (PlatformDetails.RunningOnPosix)
                    return unixInstance.State;
                return winInstance.State;
            }
        }

        public async Task ConnectAsync(Uri uri, CancellationToken token)
        {
            if (PlatformDetails.RunningOnPosix)
                await unixInstance.ConnectAsync(uri, token);
            else
                await winInstance.ConnectAsync(uri, token);
        }

        public async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> arraySegment, CancellationToken token)
        {
            if (PlatformDetails.RunningOnPosix)
                return await unixInstance.ReceiveAsync(arraySegment, token);
            return await winInstance.ReceiveAsync(arraySegment, token).ConfigureAwait(false);
        }

        public async Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken token)
        {
            if (PlatformDetails.RunningOnPosix)
                await unixInstance.CloseOutputAsync(closeStatus, statusDescription, token);
            else
                await winInstance.CloseOutputAsync(closeStatus, statusDescription, token);
        }

        public async Task SendAsync(ArraySegment<byte> segment, WebSocketMessageType messageType, bool endOfMessage, CancellationToken token)
        {
            if (PlatformDetails.RunningOnPosix)
                await unixInstance.SendAsync(segment, messageType, endOfMessage, token);
            else
                await winInstance.SendAsync(segment, messageType, endOfMessage, token);
        }

        public void Dispose()
        {
            if (PlatformDetails.RunningOnPosix)
                unixInstance.Dispose();
            else
                winInstance.Dispose();
        }

        public async Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken token)
        {
            if (PlatformDetails.RunningOnPosix)
                await unixInstance.CloseAsync(closeStatus, statusDescription, token);
            else
                await winInstance.CloseAsync(closeStatus, statusDescription, token);
        }
    }
}
