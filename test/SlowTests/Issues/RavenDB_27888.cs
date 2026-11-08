using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL.Providers.Raven;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

// A RavenDB ETL load to a destination that rejects the batch before reading all of it (e.g. a 4.2 server that cannot decode Zstd)
// must not outlive the load request timeout, and must not keep the ETL read transaction open after the task is disabled.
[Collection(UnsecuredHttp2OnlyServerCollection.Name)]
public class RavenDB_27888 : RavenTestBase
{
    private readonly HttpVersionPolicy? _defaultHttpVersionPolicy = DocumentConventions.DefaultHttpVersionPolicy;

    private RavenServer _sourceServer;

    public RavenDB_27888(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task Load_rejected_early_by_destination_fails_within_load_request_timeout()
    {
        await using var destination = await FakeDestination.StartAsync();
        using var src = GetHttp2OnlySourceStore();

        var etl = await AddEtlTo(src, destination, loadRequestTimeoutInSec: 3);
        StoreLargeOrders(src);

        Assert.True(await destination.BatchRejected.WaitAsync(TimeSpan.FromSeconds(60)), "the destination never received the batch");

        // the load request timeout is 3 seconds, so the failed load should be recorded long before this
        var loadErrorRecorded = await WaitForValueAsync(() => etl.Statistics.LastLoadErrorTime != null, true, timeout: 60_000, interval: 500);
        Assert.True(loadErrorRecorded, $"the ETL load was still running {60 + 3}s after the destination rejected the batch (load request timeout: 3s)");
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task Disabling_etl_releases_read_transaction_of_load_rejected_early_by_destination()
    {
        await using var destination = await FakeDestination.StartAsync();
        using var src = GetHttp2OnlySourceStore();

        var etl = await AddEtlTo(src, destination, loadRequestTimeoutInSec: 300);
        StoreLargeOrders(src);

        Assert.True(await destination.BatchRejected.WaitAsync(TimeSpan.FromSeconds(60)), "the destination never received the batch");

        var database = await GetSourceDatabase(src);
        Assert.NotEmpty(OpenEtlTransactions(database));

        await src.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(etl.Configuration.TaskId, OngoingTaskType.RavenEtl, disable: true));

        var stillLoaded = await WaitForValueAsync(() => database.EtlLoader.Processes.Contains(etl), false, timeout: 30_000, interval: 500);
        Assert.False(stillLoaded, "the disabled ETL process was not removed");

        var openEtlTransactions = await WaitForValueAsync(() => OpenEtlTransactions(database).Count, 0, timeout: 30_000, interval: 500);
        Assert.True(openEtlTransactions == 0, $"{openEtlTransactions} ETL read transaction(s) are still open 30s after the ETL task was disabled");
    }

    private DocumentStore GetHttp2OnlySourceStore()
    {
        // an unsecured HTTP/2-only server makes its internal clients, the ETL one included, use h2c (see RavenServer.Initialize)
        var options = new Options
        {
            Server = _sourceServer = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Http.Protocols)] = HttpProtocols.Http2.ToString() }
            }),
            ModifyDocumentStore = s => s.Conventions.HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact
        };

        return GetDocumentStore(options);
    }

    private async Task<RavenEtl> AddEtlTo(DocumentStore src, FakeDestination destination, int loadRequestTimeoutInSec)
    {
        const string name = "to-fake-destination";

        Etl.AddEtl(src, new RavenEtlConfiguration
        {
            Name = name,
            ConnectionStringName = name,
            LoadRequestTimeoutInSec = loadRequestTimeoutInSec,
            Transforms = { new Transformation { Name = "orders", Collections = { "Orders" }, Script = string.Empty } }
        }, new RavenConnectionString
        {
            Name = name,
            Database = FakeDestination.DatabaseName,
            TopologyDiscoveryUrls = new[] { destination.Url }
        });

        var database = await GetSourceDatabase(src);
        Assert.True(await WaitForValueAsync(() => database.EtlLoader.Processes.Any(x => x.ConfigurationName == name), true));
        return Assert.IsType<RavenEtl>(database.EtlLoader.Processes.Single(x => x.ConfigurationName == name));
    }

    private Task<DocumentDatabase> GetSourceDatabase(DocumentStore src) => _sourceServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(src.Database);

    private static void StoreLargeOrders(DocumentStore store)
    {
        // a few MB of poorly compressible data, so the batch outgrows the HTTP/2 flow-control windows of the destination
        var random = new Random(12345);
        var payload = new byte[16 * 1024];

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 256; i++)
            {
                random.NextBytes(payload);
                bulk.Store(new Order { Payload = Convert.ToBase64String(payload) }, $"orders/{i}");
            }
        }
    }

    private static List<string> OpenEtlTransactions(DocumentDatabase database)
    {
        return database.DocumentsStorage.Environment.ActiveTransactions.AllTransactionsInstances
            .Select(tx => tx.CurrentTransactionHolder?.Name)
            .Where(threadName => threadName != null && threadName.Contains("ETL", StringComparison.OrdinalIgnoreCase))
            .ToList();
    }

    public override void Dispose()
    {
        try
        {
            base.Dispose();
        }
        finally
        {
            DocumentConventions.DefaultHttpVersionPolicy = _defaultHttpVersionPolicy;
        }
    }

    private sealed class Order
    {
        public string Payload { get; set; }
    }

    // Answers topology requests like a single-node RavenDB server, and rejects every batch the way a 4.2 server rejects a Zstd body:
    // it reads only the start of the request body, fails to parse it, and responds with an error without consuming the rest
    // or resetting the stream.
    private sealed class FakeDestination : IAsyncDisposable
    {
        public const string DatabaseName = "dest";

        private readonly WebApplication _app;
        private readonly CancellationTokenSource _shutdown = new();

        public readonly AsyncManualResetEvent BatchRejected = new();

        public string Url { get; private set; }

        private FakeDestination(WebApplication app)
        {
            _app = app;
        }

        public static async Task<FakeDestination> StartAsync()
        {
            var builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseKestrel(k => k.Listen(System.Net.IPAddress.Loopback, 0, l => l.Protocols = HttpProtocols.Http2));

            var app = builder.Build();
            var destination = new FakeDestination(app);

            app.MapGet("/topology", (HttpContext ctx) => ctx.Response.WriteAsync(
                $"{{\"Etag\":1,\"Nodes\":[{{\"Url\":\"{destination.Url}\",\"ClusterTag\":\"A\",\"Database\":\"{DatabaseName}\",\"ServerRole\":\"Member\"}}]}}"));

            app.MapPost($"/databases/{DatabaseName}/bulk_docs", async (HttpContext ctx) =>
            {
                var start = new byte[16];
                _ = await ctx.Request.Body.ReadAsync(start);

                ctx.Response.StatusCode = StatusCodes.Status500InternalServerError;
                ctx.Response.ContentType = "application/json";
                await ctx.Response.WriteAsync("{\"Type\":\"System.IO.InvalidDataException\",\"Message\":\"Cannot have a '(' in this position\"}");
                await ctx.Response.Body.FlushAsync();

                destination.BatchRejected.Set();

                // the response is out, but the request stream stays open and half-read until the client gives up on it;
                // returning here instead makes Kestrel reset the stream, which aborts the client upload and hides the hang
                try
                {
                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(ctx.RequestAborted, destination._shutdown.Token);
                    await Task.Delay(Timeout.Infinite, cts.Token);
                }
                catch (OperationCanceledException)
                {
                }
            });

            await app.StartAsync();

            destination.Url = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First().TrimEnd('/');
            return destination;
        }

        public async ValueTask DisposeAsync()
        {
            _shutdown.Cancel();
            await _app.StopAsync();
            await _app.DisposeAsync();
            _shutdown.Dispose();
        }
    }
}
