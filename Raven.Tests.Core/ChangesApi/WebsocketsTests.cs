using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Json.Linq;
using Raven.Tests.Core.BulkInsert;
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

	    public class Node
	    {
		    public string Name { get; set; }
	    }

	    [Fact]
	    public void AreWebsocketsDestroyedAfterGC()
	    {
		    var counter = new ConcurrentQueue<BulkInsertChangeNotification>();
			var are = new AutoResetEvent(false);
		    using (var store = GetDocumentStore())
		    {
			    Task[] tasks = new Task[10];
			    for (int i = 0; i < 10; i++)
			    {
				    tasks[i] = Task.Factory.StartNew(() =>
				    {
						using (var bulkInsert = store.BulkInsert(store.DefaultDatabase))
					    {
						    store.Changes().ForBulkInsert(bulkInsert.OperationId).Subscribe(x =>
						    {
							    counter.Enqueue(x);
							    are.Set();
						    });
						    for (int j = 0; j < 50; j++)
						    {
							    bulkInsert.Store(new ChunkedBulkInsert.Node
							    {
									Name = "Parent"
							    });
						    }
					    }
				    });
			    }
			    are.WaitOne();
			    

				var gcRequest = store
					.JsonRequestFactory
					.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
						store.Url.ForDatabase(null) + "/admin/gc", 
						"GET", 
						store.DatabaseCommands.PrimaryCredentials, 
						store.Conventions));
				var gcResponse = gcRequest.ReadResponseBytesAsync();
			    gcResponse.Wait();

				var getChangesRequest = store
					.JsonRequestFactory
					.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
						store.Url.ForDatabase(store.DefaultDatabase) + "/debug/changes",
						"GET",
						store.DatabaseCommands.PrimaryCredentials,
						store.Conventions));
				var getChangesResponse = (RavenJArray)getChangesRequest.ReadResponseJson();

				Assert.Empty(getChangesResponse);
				

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