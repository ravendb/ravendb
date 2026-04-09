using System;
using System.Net;
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
            using var listener = new HttpListener();
            var port = GetAvailablePort();
            var prefix = $"http://127.0.0.1:{port}/";
            listener.Prefixes.Add(prefix);
            listener.Start();

            _ = Task.Run(async () =>
            {
                while (listener.IsListening)
                {
                    try
                    {
                        var ctx = await listener.GetContextAsync();
                        var response = Encoding.UTF8.GetBytes("<html><body>Cats are cute</body></html>");
                        ctx.Response.ContentType = "text/html";
                        ctx.Response.ContentLength64 = response.Length;
                        await ctx.Response.OutputStream.WriteAsync(response, 0, response.Length);
                        ctx.Response.Close();
                    }
                    catch (ObjectDisposedException)
                    {
                        break;
                    }
                }
            });

            using (var store = new DocumentStore
            {
                Urls = new[] { prefix.TrimEnd('/') },
                Database = "test"
            })
            {
                store.Initialize();

                var e = await Assert.ThrowsAnyAsync<Exception>(async () =>
                {
                    await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                });

                // Should not be NullReferenceException
                Assert.DoesNotContain("NullReferenceException", e.ToString());
                // Should mention that the URL may not point to a RavenDB server
                Assert.Contains("Cats are cute", e.ToString());
            }

            listener.Stop();
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Should_Throw_Informative_Error_When_Server_Returns_Unrelated_Json()
        {
            using var listener = new HttpListener();
            var port = GetAvailablePort();
            var prefix = $"http://127.0.0.1:{port}/";
            listener.Prefixes.Add(prefix);
            listener.Start();

            _ = Task.Run(async () =>
            {
                while (listener.IsListening)
                {
                    try
                    {
                        var ctx = await listener.GetContextAsync();
                        var response = Encoding.UTF8.GetBytes("{\"status\":\"ok\",\"service\":\"some-other-api\",\"version\":\"2.0\"}");
                        ctx.Response.ContentType = "application/json";
                        ctx.Response.ContentLength64 = response.Length;
                        await ctx.Response.OutputStream.WriteAsync(response, 0, response.Length);
                        ctx.Response.Close();
                    }
                    catch (ObjectDisposedException)
                    {
                        break;
                    }
                }
            });

            using (var store = new DocumentStore
            {
                Urls = new[] { prefix.TrimEnd('/') },
                Database = "test"
            })
            {
                store.Initialize();

                var e = await Assert.ThrowsAnyAsync<Exception>(async () =>
                {
                    await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                });

                // Should not be NullReferenceException
                Assert.DoesNotContain("NullReferenceException", e.ToString());
                // Should contain the JSON response body from the server
                Assert.Contains("some-other-api", e.ToString());
            }

            listener.Stop();
        }

        private static int GetAvailablePort()
        {
            var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }
    }
}
