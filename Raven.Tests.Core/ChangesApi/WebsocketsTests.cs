#if !DNXCORE50
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Core.BulkInsert;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Core.ChangesApi
{
    public class WebsocketsTests : RavenTestBase
    {
        [Fact]
        public async Task Can_connect_via_websockets_and_receive_heartbeat()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var clientWebSocket = TryCreateClientWebSocket())
                {
                    var url = store.Url.Replace("http:", "ws:");
                    url = url + "/changes/websocket?id=" + Guid.NewGuid();
                    await clientWebSocket.ConnectAsync(new Uri(url), CancellationToken.None);

                    var buffer = new byte[1024];
                    var result = await clientWebSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                    var message = Encoding.UTF8.GetString(buffer, 0, result.Count);

                    Assert.Contains("Heartbeat", message);
                }
            }
        }

        public class Node
        {
            public string Name { get; set; }
        }

        [Fact(Skip = "Known failure")]
        public void AreWebsocketsDestroyedAfterGC()
        {
            var counter = new ConcurrentQueue<BulkInsertChangeNotification>();

            using (var store = NewRemoteDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert(store.DefaultDatabase))
                {
                    var testTimer = Stopwatch.StartNew();
                    store.Changes().Task.Result.ForBulkInsert(bulkInsert.OperationId).Task.Result.Subscribe(counter.Enqueue);

                    bulkInsert.Store(new ChunkedBulkInsert.Node
                    {
                        Name = "Parent"
                    });

                    IssueGCRequest(store);

                    bulkInsert.Store(new ChunkedBulkInsert.Node
                    {
                        Name = "Parent"
                    });

                    const int maxMillisecondsToWaitUntilConnectionRestores = 5000;

                    //wait until connection restores
                    RavenJArray response;
                    var sw = Stopwatch.StartNew();

                    int retryCount = 0;
                    do
                    {
                        response = IssueGetChangesRequest(store);
                        retryCount++;
                    }
                    while (response == null || response.Length == 0 || sw.ElapsedMilliseconds <= maxMillisecondsToWaitUntilConnectionRestores);

                    //sanity check, if the test fails here, then something is wrong
                    // if it is null or empty then it means the connection did not restore after 1 second by itself. Should be investigated.
                    Assert.NotEmpty(response);

                    var connectionAge = TimeSpan.Parse(response.First().Value<string>("Age"));
                    var timeSinceTestStarted = TimeSpan.FromMilliseconds(testTimer.ElapsedMilliseconds);

                    Assert.True(connectionAge < timeSinceTestStarted);
                }
            }
        }

        private static DateTime GetLastForcedGCDateTimeRequest(DocumentStore store)
        {
            var request = store.JsonRequestFactory.CreateHttpJsonRequest(
                new CreateHttpJsonRequestParams(null,
                    store.Url + "/debug/gc-info",
                    HttpMethod.Get,
                    store.DatabaseCommands.PrimaryCredentials,
                    store.Conventions));

            var response = request.ReadResponseJson();
            return response.Value<DateTime>("LastForcedGCTime");

        }

        private static RavenJArray IssueGetChangesRequest(DocumentStore store)
        {
            var getChangesRequest = store
                .JsonRequestFactory
                .CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
                    store.Url.ForDatabase(store.DefaultDatabase) + "/debug/changes",
                    HttpMethod.Get,
                    store.DatabaseCommands.PrimaryCredentials,
                    store.Conventions));

            var getChangesResponse = (RavenJArray)getChangesRequest.ReadResponseJson();
            return getChangesResponse;
        }

        private static void IssueGCRequest(DocumentStore store)
        {
            var gcRequest = store
                .JsonRequestFactory
                .CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
                    store.Url.ForDatabase(null) + "/admin/gc",
                    HttpMethod.Get,
                    store.DatabaseCommands.PrimaryCredentials,
                    store.Conventions));
            var gcResponse = gcRequest.ReadResponseBytesAsync();
            gcResponse.Wait();
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
#endif