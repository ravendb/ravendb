using System;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Extensions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16510 : ReplicationTestBase
    {
        public RavenDB_16510(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CheckTcpTrafficWatch(Options options)
        {
            DoNotReuseServer();

            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                var cts = new CancellationTokenSource();

                var readFromSocketTask = Task.Run(async () =>
                {
                    using (var clientWebSocket = new ClientWebSocket())
                    {
                        string url = store1.Urls.First().Replace("http://", "ws://");
                        await clientWebSocket.ConnectAsync(new Uri($"{url}/admin/traffic-watch"), cts.Token);
                        Assert.Equal(WebSocketState.Open, clientWebSocket.State);

                        var arraySegment = new ArraySegment<byte>(new byte[512]);
                        var buffer = new StringBuilder();
                        var charBuffer = new char[Encodings.Utf8.GetMaxCharCount(arraySegment.Count)];

                        while (cts.IsCancellationRequested == false)
                        {
                            buffer.Length = 0;
                            WebSocketReceiveResult result;

                            do
                            {
                                result = await clientWebSocket.ReceiveAsync(arraySegment, cts.Token);
                                var chars = Encodings.Utf8.GetChars(arraySegment.Array, 0, result.Count, charBuffer, 0);
                                buffer.Append(charBuffer, 0, chars);
                            } while (!result.EndOfMessage);

                            if (result.Count <= 2) continue;

                            var msg = buffer.ToString();
                            JObject json = JObject.Parse(msg);
                            var msgType = json.Value<string>("TrafficWatchType");
                            var databaseName = json.Value<string>("DatabaseName") ?? "N/A";
                            var operation = json.Value<string>("Operation");

                            if (msgType is not "Tcp" || databaseName.Equals("N/A") || operation is not "Replication") continue;

                            Assert.True(databaseName.Equals(store1.Database));
                            return;
                        }
                    }
                });

                await SetupReplicationAsync(store2, store1);
                using (var session = store2.OpenSession())
                {
                    session.Store(new User { Name = "Jane Dow", Age = 31 }, "users/2");
                    session.SaveChanges();
                }

                cts.CancelAfter(TimeSpan.FromMinutes(1));

                Assert.True(await readFromSocketTask.WaitWithTimeout(TimeSpan.FromMinutes(2)));
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CheckTcpTrafficWatchExceptionMessage(Options options, bool exceptionType)
        {
            DoNotReuseServer();

            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                var cts = new CancellationTokenSource();

                var readFromSocketTask = Task.Run(async () =>
                {
                    using (var clientWebSocket = new ClientWebSocket())
                    {
                        string url = store1.Urls.First().Replace("http://", "ws://");
                        await clientWebSocket.ConnectAsync(new Uri($"{url}/admin/traffic-watch"), cts.Token);
                        Assert.Equal(WebSocketState.Open, clientWebSocket.State);

                        var arraySegment = new ArraySegment<byte>(new byte[512]);
                        var buffer = new StringBuilder();
                        var charBuffer = new char[Encodings.Utf8.GetMaxCharCount(arraySegment.Count)];

                        while (cts.IsCancellationRequested == false)
                        {
                            buffer.Length = 0;
                            WebSocketReceiveResult result;

                            do
                            {
                                result = await clientWebSocket.ReceiveAsync(arraySegment, cts.Token);
                                var chars = Encodings.Utf8.GetChars(arraySegment.Array, 0, result.Count, charBuffer, 0);
                                buffer.Append(charBuffer, 0, chars);
                            } while (!result.EndOfMessage);

                            if (result.Count <= 2) continue;

                            var msg = buffer.ToString();
                            JObject json = JObject.Parse(msg);
                            var msgType = json.Value<string>("TrafficWatchType");
                            var customInfo = json.Value<string>("CustomInfo") ?? "N/A";

                            if (msgType is not "Tcp" || customInfo is "N/A") continue;

                            Assert.True(customInfo.Contains("Simulated TCP failure."));
                            return;
                        }
                    }
                });

                if (exceptionType)
                    Server.ForTestingPurposesOnly().ThrowExceptionInListenToNewTcpConnection = true;
                else
                    Server.ForTestingPurposesOnly().ThrowExceptionInTrafficWatchTcp = true;

                await SetupReplicationAsync(store2, store1);

                using (var session = store2.OpenSession())
                {
                    session.Store(new User { Name = "Jane Dow", Age = 31 }, "users/2");
                    session.SaveChanges();
                }

                cts.CancelAfter(TimeSpan.FromMinutes(1));

                Assert.True(await readFromSocketTask.WaitWithTimeout(TimeSpan.FromMinutes(2)));
            }
        }
    }
}
