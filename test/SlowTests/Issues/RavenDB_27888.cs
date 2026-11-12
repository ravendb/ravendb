using System;
using System.Collections.Generic;
using System.IO;
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

// A request to a server that responds before reading the whole request body, and leaves the HTTP/2 stream half-read (e.g. a 4.2 server
// that cannot decode Zstd) must fail exactly like the equivalent request to a well-behaved server: with the server's error when the response
// is complete, with a timeout or cancellation when it is not, and without keeping the ETL read transaction open after the task is disabled.
[Collection(UnsecuredHttp2OnlyServerCollection.Name)]
public class RavenDB_27888 : RavenTestBase
{
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(1);

    // the upper bound of every wait, which only a failing test reaches
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(15);

    private readonly HttpVersionPolicy? _defaultHttpVersionPolicy = DocumentConventions.DefaultHttpVersionPolicy;

    private RavenServer _sourceServer;

    public RavenDB_27888(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task Request_rejected_early_with_complete_response_fails_with_the_server_error()
    {
        await using var destination = await FakeDestination.StartAsync();

        destination.Behavior = BatchBehavior.ReadWholeBodyThenReject;
        var expected = await SaveLargeBatchAndGetErrorAsync(destination, WaitTimeout);

        destination.Behavior = BatchBehavior.RejectEarlyAndCompleteResponse;
        var actual = await SaveLargeBatchAndGetErrorAsync(destination, WaitTimeout);

        AssertSameError(expected, actual);
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task Request_rejected_early_with_incomplete_response_times_out_like_a_request_that_gets_no_response()
    {
        await using var destination = await FakeDestination.StartAsync();

        destination.Behavior = BatchBehavior.NeverRespond;
        var expected = await SaveLargeBatchAndGetErrorAsync(destination, RequestTimeout);

        destination.Behavior = BatchBehavior.RejectEarlyAndKeepResponseOpen;
        var actual = await SaveLargeBatchAndGetErrorAsync(destination, RequestTimeout);

        AssertRespondedBeforeTheRequestFailed(destination);

        AssertSameError(expected, actual);
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task Request_rejected_early_with_incomplete_response_is_canceled_like_a_request_that_gets_no_response()
    {
        await using var destination = await FakeDestination.StartAsync();

        destination.Behavior = BatchBehavior.NeverRespond;
        var expected = await SaveLargeBatchAndGetErrorAsync(destination, requestTimeout: null, cancelAfter: RequestTimeout);

        destination.Behavior = BatchBehavior.RejectEarlyAndKeepResponseOpen;
        var actual = await SaveLargeBatchAndGetErrorAsync(destination, requestTimeout: null, cancelAfter: RequestTimeout);

        AssertRespondedBeforeTheRequestFailed(destination);

        Assert.IsAssignableFrom<OperationCanceledException>(expected);
        AssertSameError(expected, actual);
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task Load_rejected_early_by_destination_fails_within_load_request_timeout()
    {
        await using var destination = await FakeDestination.StartAsync();
        using var src = GetHttp2OnlySourceStore();

        var etl = await AddEtlTo(src, destination, loadRequestTimeoutInSec: (int)RequestTimeout.TotalSeconds);
        StoreLargeOrders(src);

        Assert.True(await destination.BatchReceived.WaitAsync(WaitTimeout), "the destination never received the batch");

        var loadErrorRecorded = await WaitForValueAsync(() => etl.Statistics.LastLoadErrorTime != null, true, timeout: (int)WaitTimeout.TotalMilliseconds, interval: 100);
        Assert.True(loadErrorRecorded, $"the ETL load was still running {WaitTimeout} after the destination rejected the batch (load request timeout: {RequestTimeout})");
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task Disabling_etl_releases_read_transaction_of_load_rejected_early_by_destination()
    {
        await using var destination = await FakeDestination.StartAsync();
        using var src = GetHttp2OnlySourceStore();

        var etl = await AddEtlTo(src, destination, loadRequestTimeoutInSec: 300);
        StoreLargeOrders(src);

        Assert.True(await destination.BatchReceived.WaitAsync(WaitTimeout), "the destination never received the batch");

        var database = await GetSourceDatabase(src);
        Assert.NotEmpty(OpenEtlTransactions(database));

        await src.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(etl.Configuration.TaskId, OngoingTaskType.RavenEtl, disable: true));

        var stillLoaded = await WaitForValueAsync(() => database.EtlLoader.Processes.Contains(etl), false, timeout: (int)WaitTimeout.TotalMilliseconds, interval: 100);
        Assert.False(stillLoaded, "the disabled ETL process was not removed");

        var openEtlTransactions = await WaitForValueAsync(() => OpenEtlTransactions(database).Count, 0, timeout: (int)WaitTimeout.TotalMilliseconds, interval: 100);
        Assert.True(openEtlTransactions == 0, $"{openEtlTransactions} ETL read transaction(s) are still open {WaitTimeout} after the ETL task was disabled");
    }

    private static async Task<Exception> SaveLargeBatchAndGetErrorAsync(FakeDestination destination, TimeSpan? requestTimeout, TimeSpan? cancelAfter = null)
    {
        using var store = new DocumentStore
        {
            Urls = new[] { destination.Url },
            Database = FakeDestination.DatabaseName,
            Conventions =
            {
                DisableTopologyUpdates = true,
                HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact,
                RequestTimeout = requestTimeout
            }
        }.Initialize();

        using var cts = new CancellationTokenSource();
        if (cancelAfter != null)
            cts.CancelAfter(cancelAfter.Value);

        // not disposed when the request hangs, since disposing a session with an active request throws and hides the failure
        var session = store.OpenAsyncSession();
        foreach (var (id, order) in LargeOrders())
            await session.StoreAsync(order, id);

        var saveChanges = session.SaveChangesAsync(cts.Token);

        // a request that never completes must not hang the test
        var completed = await Task.WhenAny(saveChanges, Task.Delay(WaitTimeout + (requestTimeout ?? cancelAfter ?? TimeSpan.Zero)));
        Assert.True(completed == saveChanges, $"the request did not complete in time (behavior: {destination.Behavior})");

        session.Dispose();

        var exception = await Record.ExceptionAsync(() => saveChanges);
        Assert.True(exception != null, $"the request was expected to fail (behavior: {destination.Behavior})");
        return exception;
    }

    private static void AssertRespondedBeforeTheRequestFailed(FakeDestination destination)
    {
        // otherwise the request timed out or was canceled before the response arrived, and the test checks nothing
        Assert.True(destination.RespondedEarly, "the destination did not respond before the request failed");
    }

    private static void AssertSameError(Exception expected, Exception actual)
    {
        Assert.True(Describe(expected).SequenceEqual(Describe(actual)),
            $"Expected:{Environment.NewLine}{expected}{Environment.NewLine}{Environment.NewLine}Actual:{Environment.NewLine}{actual}");

        // The type and the first message line of each exception down to the cancellation, if any. Error messages embed the stack traces
        // of their inner exceptions, and the inner exceptions of a cancellation are internal to the HTTP handler.
        static IEnumerable<string> Describe(Exception e)
        {
            for (; e != null; e = e.InnerException)
            {
                yield return $"{e.GetType()}: {e.Message.Split(Environment.NewLine)[0]}";

                if (e is OperationCanceledException)
                    yield break;
            }
        }
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
        using (var bulk = store.BulkInsert())
        {
            foreach (var (id, order) in LargeOrders())
                bulk.Store(order, id);
        }
    }

    private static IEnumerable<(string Id, Order Order)> LargeOrders()
    {
        // a few MB of poorly compressible data, so the batch outgrows the HTTP/2 flow-control windows of the destination
        var random = new Random(12345);
        var payload = new byte[16 * 1024];

        for (int i = 0; i < 256; i++)
        {
            random.NextBytes(payload);
            yield return ($"orders/{i}", new Order { Payload = Convert.ToBase64String(payload) });
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

    private enum BatchBehavior
    {
        // the way a 4.2 server rejects a Zstd body: it reads only the start of the request body, fails to parse it, and responds with an error
        // without consuming the rest or resetting the stream
        RejectEarlyAndKeepResponseOpen,

        // like RejectEarlyAndKeepResponseOpen, but the response is complete (END_STREAM), while the request stream stays open and half-read
        RejectEarlyAndCompleteResponse,

        // a well-behaved server rejecting the batch
        ReadWholeBodyThenReject,

        // a server that never responds
        NeverRespond
    }

    // Answers topology requests like a single-node RavenDB server, and handles every batch according to its Behavior
    private sealed class FakeDestination : IAsyncDisposable
    {
        public const string DatabaseName = "dest";

        private const string Error = "{\"Type\":\"System.IO.InvalidDataException\",\"Message\":\"Cannot have a '(' in this position\",\"Error\":\"System.IO.InvalidDataException: Cannot have a '(' in this position\"}";

        private readonly WebApplication _app;
        private readonly CancellationTokenSource _shutdown = new();

        public readonly AsyncManualResetEvent BatchReceived = new();

        public volatile BatchBehavior Behavior = BatchBehavior.RejectEarlyAndKeepResponseOpen;

        public volatile bool RespondedEarly;

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

            app.MapPost($"/databases/{DatabaseName}/bulk_docs", destination.HandleBatchAsync);

            await app.StartAsync();

            destination.Url = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First().TrimEnd('/');
            return destination;
        }

        private async Task HandleBatchAsync(HttpContext ctx)
        {
            switch (Behavior)
            {
                case BatchBehavior.ReadWholeBodyThenReject:
                    await ctx.Request.Body.CopyToAsync(Stream.Null);
                    await WriteErrorAsync(ctx);
                    BatchReceived.Set();
                    return;

                case BatchBehavior.NeverRespond:
                    BatchReceived.Set();
                    await HangUntilAbortedAsync(ctx);
                    return;

                case BatchBehavior.RejectEarlyAndKeepResponseOpen:
                case BatchBehavior.RejectEarlyAndCompleteResponse:
                    var start = new byte[16];
                    _ = await ctx.Request.Body.ReadAsync(start);

                    await WriteErrorAsync(ctx);

                    if (Behavior == BatchBehavior.RejectEarlyAndCompleteResponse)
                        await ctx.Response.CompleteAsync();
                    else
                        await ctx.Response.Body.FlushAsync();

                    RespondedEarly = true;
                    BatchReceived.Set();

                    // the response is out, but the request stream stays open and half-read until the client gives up on it;
                    // returning here instead makes Kestrel reset the stream, which aborts the client upload and hides the hang
                    await HangUntilAbortedAsync(ctx);
                    return;

                default:
                    throw new ArgumentOutOfRangeException(nameof(Behavior), Behavior, null);
            }
        }

        private static Task WriteErrorAsync(HttpContext ctx)
        {
            ctx.Response.StatusCode = StatusCodes.Status500InternalServerError;
            ctx.Response.ContentType = "application/json";
            return ctx.Response.WriteAsync(Error);
        }

        private async Task HangUntilAbortedAsync(HttpContext ctx)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ctx.RequestAborted, _shutdown.Token);
                await Task.Delay(Timeout.Infinite, cts.Token);
            }
            catch (OperationCanceledException)
            {
            }
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
