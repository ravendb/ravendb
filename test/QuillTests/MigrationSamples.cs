using System.IO.Compression;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper;
using Raven.Quill.AiHelper.Migration;
using Raven.Quill.AiHelper.Migration.Planning;
using Raven.Quill.Wizard;

namespace QuillTests;

/// <summary>Builders for the migration-agent tests: a configuration that validates, and a planner to talk to.</summary>
public static class MigrationSamples
{
    private static readonly JsonSerializerOptions WireOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() },
        IncludeFields = true,
    };

    public static CdcSinkTableConfig ValidOrders() => new()
    {
        CollectionName = "Orders",
        SourceTableSchema = "public",
        SourceTableName = "orders",
        PrimaryKeyColumns = ["order_id"],
        Columns =
        [
            new CdcColumnMapping { Column = "order_id", Name = "OrderId" },
            new CdcColumnMapping { Column = "ordered_at", Name = "OrderedAt" }
        ]
    };

    public static CdcSinkEmbeddedTableConfig ValidLines() => new()
    {
        SourceTableSchema = "public",
        SourceTableName = "order_lines",
        PropertyName = "Lines",
        Type = CdcSinkRelationType.Array,
        JoinColumns = ["order_id"],
        PrimaryKeyColumns = ["order_line_id"],
        Columns =
        [
            new CdcColumnMapping { Column = "order_line_id", Name = "LineId" },
            new CdcColumnMapping { Column = "quantity", Name = "Quantity" }
        ]
    };

    public static CdcSinkLinkedTableConfig ValidCustomer() => new()
    {
        SourceTableSchema = "public",
        SourceTableName = "customers",
        PropertyName = "Customer",
        LinkedCollectionName = "Customers",
        JoinColumns = ["customer_id"]
    };

    public static async Task SeedDiscoveredSchemaAsync(
        IDocumentStore store,
        string slug,
        MigrationPlan? plan = null,
        CdcSinkConfiguration? mapConfiguration = null)
    {
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new WizardState
        {
            Provider = "SqlClient",
            LastDiscoveredSchema = new CdcSinkSourceSchema
            {
                CatalogName = "shop",
                HasPermissionToSetup = true,
                Tables = [SourceTable("orders"), SourceTable("customers"), SourceTable("audit_log")]
            },
            LastDiscoverAt = DateTime.UtcNow,
            LastMapConfiguration = mapConfiguration,
            MigrationPlan = plan
        }, WizardState.DocumentIdFor(slug));

        await session.SaveChangesAsync();
    }

    public static async Task<MigrationPlan?> LoadPlanAsync(IDocumentStore store, string slug)
    {
        using var session = store.OpenAsyncSession();
        var state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(slug));
        return state?.MigrationPlan;
    }

    public static MigrationPlan Plan(string conversationId, params PlanEntry[] entries) =>
        new() { ConversationId = conversationId, Entries = [.. entries] };

    public static string SentSchemaJson(string startRequestBody)
    {
        using var body = JsonDocument.Parse(startRequestBody);
        var compressed = Convert.FromBase64String(body.RootElement.GetProperty("SchemaGzip").GetString()!);

        using var gzip = new GZipStream(new MemoryStream(compressed), CompressionMode.Decompress);
        using var reader = new StreamReader(gzip, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    public static string Sse(params MigrationFrame[] frames) =>
        ": keepalive\n\n" + string.Concat(frames.Select(f => $"data: {JsonSerializer.Serialize(f, WireOptions)}\n\n: keepalive\n\n"));

    private static CdcSinkSourceTable SourceTable(string name) => new()
    {
        SourceTableSchema = "public",
        SourceTableName = name,
        IsCdcEnabled = true,
        PrimaryKeyColumns = ["order_id"],
        Columns =
        [
            new CdcSinkSourceColumn { Name = "order_id", NativeType = "int", IsPrimaryKey = true, IsCdcCapturable = true },
            new CdcSinkSourceColumn { Name = "ordered_at", NativeType = "timestamp", IsCdcCapturable = true }
        ]
    };
}

public sealed class StubPlannerHandler(HttpStatusCode status, string body) : HttpMessageHandler
{
    public string? LastPath { get; private set; }

    public string LastBody { get; private set; } = string.Empty;

    public static StubPlannerHandler Replying(params MigrationFrame[] frames) =>
        new(HttpStatusCode.OK, MigrationSamples.Sse(frames));

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        LastPath = request.RequestUri!.AbsolutePath;
        LastBody = await request.Content!.ReadAsStringAsync(cancellationToken);

        return new HttpResponseMessage(status)
        {
            Content = new StringContent(body, Encoding.UTF8, status == HttpStatusCode.OK ? "text/event-stream" : "application/json")
        };
    }
}

public sealed class ThrowingPlannerHandler(Exception failure) : HttpMessageHandler
{
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        Task.FromException<HttpResponseMessage>(failure);
}

public sealed class BreakingStreamPlannerHandler(string before) : HttpMessageHandler
{
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StreamContent(new BreakingStream(Encoding.UTF8.GetBytes(before)))
        });

    private sealed class BreakingStream(byte[] before) : MemoryStream(before)
    {
        public override int Read(byte[] buffer, int offset, int count) =>
            Position < Length ? base.Read(buffer, offset, count) : throw new IOException("connection reset");

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            Position < Length ? base.ReadAsync(buffer, cancellationToken) : throw new IOException("connection reset");

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            Position < Length ? base.ReadAsync(buffer, offset, count, cancellationToken) : throw new IOException("connection reset");
    }
}

public sealed class FakeConsentClient(AiHelperStatus consent) : IAiHelperClient
{
    public Task<AiHelperStatus> CheckConsentAsync(CancellationToken ct) => Task.FromResult(consent);

    public Task<AiHelperStatus> GiveConsentAsync(CancellationToken ct) => Task.FromResult(AiHelperStatus.Success);

    public Task<SuggestCdcInternalResult> SuggestCdcAsync(object? schema, object? samples, string prompt, CancellationToken ct) =>
        throw new NotSupportedException();

    public Task<SuggestAiAgentInternalResult> SuggestAiAgentAsync(
        CdcSinkConfiguration cdcConfig, object? collectionsSample, string mode, string? prompt, CancellationToken ct) =>
        throw new NotSupportedException();

    public Task<HttpResponseMessage> SendChatAsync(string message, string? conversationId, CancellationToken ct) =>
        throw new NotSupportedException();

    public Task<(AiHelperStatus Transport, string Content)> SendAsync(string path, string method, object request, CancellationToken ct) =>
        throw new NotSupportedException();

    public Task<T> DeserializeAsync<T>(string json, CancellationToken ct) where T : class =>
        Task.FromResult(JsonSerializer.Deserialize<T>(json)!);
}
