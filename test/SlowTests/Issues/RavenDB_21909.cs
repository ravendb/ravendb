using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21909 : RavenTestBase
    {
        public RavenDB_21909(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Should_Throw_Informative_Error_When_Using_Non_RavenDB_Url()
        {
            await using var server = new FakeHttpServer("<html><body>Cats are cute</body></html>", "text/html");

            using (var store = new DocumentStore
            {
                Urls = new[] { server.Url },
                Database = "test"
            })
            {
                store.Initialize();

                var e = await Assert.ThrowsAnyAsync<Exception>(async () =>
                {
                    await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                });

                Assert.DoesNotContain("NullReferenceException", e.ToString());
                // Should include the actual response body and indicate it's not a RavenDB server
                Assert.Contains("does not point to a RavenDB server", e.ToString());
                Assert.Contains("Cats are cute", e.ToString());
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Should_Throw_Informative_Error_When_Server_Returns_Unrelated_Json()
        {
            await using var server = new FakeHttpServer("{\"status\":\"ok\",\"service\":\"some-other-api\",\"version\":\"2.0\"}", "application/json");

            using (var store = new DocumentStore
            {
                Urls = new[] { server.Url },
                Database = "test"
            })
            {
                store.Initialize();

                var e = await Assert.ThrowsAnyAsync<Exception>(async () =>
                {
                    await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                });

                Assert.DoesNotContain("NullReferenceException", e.ToString());
                // Should include the actual response body and indicate it's not a RavenDB server
                Assert.Contains("does not point to a RavenDB server", e.ToString());
                Assert.Contains("some-other-api", e.ToString());
            }
        }

        private sealed class FakeHttpServer : IAsyncDisposable
        {
            private readonly TcpListener _listener;
            private readonly Task _acceptLoop;
            private readonly string _body;
            private readonly string _contentType;
            private volatile bool _stopped;

            public string Url { get; }

            public FakeHttpServer(string body, string contentType)
            {
                _body = body;
                _contentType = contentType;
                _listener = new TcpListener(IPAddress.Loopback, 0);
                _listener.Start();
                var port = ((IPEndPoint)_listener.LocalEndpoint).Port;
                Url = $"http://127.0.0.1:{port}";
                _acceptLoop = Task.Run(AcceptLoopAsync);
            }

            private async Task AcceptLoopAsync()
            {
                while (_stopped == false)
                {
                    TcpClient client;
                    try
                    {
                        client = await _listener.AcceptTcpClientAsync();
                    }
                    catch (ObjectDisposedException)
                    {
                        break;
                    }
                    catch (SocketException)
                    {
                        break;
                    }

                    using (client)
                    await using (var stream = client.GetStream())
                    using (var reader = new StreamReader(stream, Encoding.ASCII, false, 1024, leaveOpen: true))
                    {
                        // read until empty line (end of HTTP headers)
                        string line;
                        while ((line = await reader.ReadLineAsync()) != null && line.Length > 0)
                        {
                        }

                        var bodyBytes = Encoding.UTF8.GetBytes(_body);
                        var responseHeader = $"HTTP/1.1 200 OK\r\nContent-Type: {_contentType}\r\nContent-Length: {bodyBytes.Length}\r\nConnection: close\r\n\r\n";
                        var headerBytes = Encoding.ASCII.GetBytes(responseHeader);
                        await stream.WriteAsync(headerBytes, 0, headerBytes.Length);
                        await stream.WriteAsync(bodyBytes, 0, bodyBytes.Length);
                    }
                }
            }

            public async ValueTask DisposeAsync()
            {
                _stopped = true;
                _listener.Stop();
                await _acceptLoop;
            }
        }
    }
}
