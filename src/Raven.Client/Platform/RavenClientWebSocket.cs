using System;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Platform.Unix;

namespace Raven.Client.Platform
{
    public class RavenClientWebSocket : IDisposable
    {
        private RavenUnixClientWebSocket unixInstance;
        private ClientWebSocket winInstance;

        public RavenClientWebSocket()
        {
            if (Sparrow.Platform.Platform.RunningOnPosix)
                unixInstance = new RavenUnixClientWebSocket();
            else
                winInstance = new ClientWebSocket();
        }

        public WebSocketState State
        {
            get
            {
                if (Sparrow.Platform.Platform.RunningOnPosix)
                    return unixInstance.State;
                return winInstance.State;
            }
        }

        public async Task ConnectAsync(Uri uri, CancellationToken token)
        {
            if (Sparrow.Platform.Platform.RunningOnPosix)
                await unixInstance.ConnectAsync(uri, token);
            else
                await winInstance.ConnectAsync(uri, token);
        }

        public async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> arraySegment, CancellationToken token)
        {
            if (Sparrow.Platform.Platform.RunningOnPosix)
                return await unixInstance.ReceiveAsync(arraySegment, token);
            return await winInstance.ReceiveAsync(arraySegment, token);
        }

        public async Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken token)
        {
            if (Sparrow.Platform.Platform.RunningOnPosix)
                await unixInstance.CloseOutputAsync(closeStatus, statusDescription, token);
            else
                await winInstance.CloseOutputAsync(closeStatus, statusDescription, token);
        }

        public async Task SendAsync(ArraySegment<byte> segment, WebSocketMessageType messageType, bool endOfMessage, CancellationToken token)
        {
            if (Sparrow.Platform.Platform.RunningOnPosix)
                await unixInstance.SendAsync(segment, messageType, endOfMessage, token);
            else
                await winInstance.SendAsync(segment, messageType, endOfMessage, token);
        }

        public void Dispose()
        {
            if (Sparrow.Platform.Platform.RunningOnPosix)
                unixInstance.Dispose();
            else
                winInstance.Dispose();
        }

        public async Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken token)
        {
            if (Sparrow.Platform.Platform.RunningOnPosix)
                await unixInstance.CloseAsync(closeStatus, statusDescription, token);
            else
                await winInstance.CloseAsync(closeStatus, statusDescription, token);
        }
    }
}
