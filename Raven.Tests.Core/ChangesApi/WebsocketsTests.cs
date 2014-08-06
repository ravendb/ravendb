using System;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Tests.Core.Replication;
using Xunit;

namespace Raven.Tests.Core.ChangesApi
{
    public class WebsocketsTests : RavenReplicationCoreTest
    {
        [Fact]
        public async Task Can_connect_via_websockets_and_receive_heartbeat()
        {
            using (var store = GetDocumentStore())
            {
                using (var clientWebSocket = TryCreateClientWebSocket())
                {
                    string url = store.Url.Replace("http:", "ws:");
                    url = url + "/changes/websocket?id=" + Guid.NewGuid();
                    await clientWebSocket.ConnectAsync(new Uri(url), CancellationToken.None);

                    var buffer = new byte[1024];
                    WebSocketReceiveResult result =
                        await clientWebSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                    var message = Encoding.UTF8.GetString(buffer, 0, result.Count);

                    Assert.Contains("Heartbeat", message);
                }
            }
        }

	    private static ClientWebSocket TryCreateClientWebSocket()
	    {
		    try
		    {
			    return new ClientWebSocket();
		    }
		    catch (PlatformNotSupportedException)
		    {
			    throw new SkipException("Cannot run this test on this platform");
		    }
	    }
    }
}