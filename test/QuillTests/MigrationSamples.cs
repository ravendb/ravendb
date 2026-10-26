using System.IO.Compression;
using System.Reflection;
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
using Raven.Quill.Contracts;
using Raven.Quill.Wizard;
using Raven.Server.Documents.AI.AiAssistant.Handlers;
using Raven.Server.Routing;

namespace QuillTests;

/// <summary>Builders for the migration-agent tests: a configuration that validates, a plan as the api answers it, and a planner to talk to.</summary>
internal static class MigrationSamples
{
    public const string PlanId = "planner/11111111-1111-1111-1111-111111111111";

    public static string ServerPath(string handlerMethod) =>
        typeof(AiAssistantHandler).GetMethod(handlerMethod)!.GetCustomAttribute<RavenActionAttribute>()!.Path;

    private static readonly JsonSerializerOptions WireOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() },
        IncludeFields = true,
    };

    private static readonly JsonSerializerOptions ApiOptions = new()
    {
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

    public static CdcSinkTableConfig ValidCustomers()
    {
        var customers = ValidOrders();
        customers.CollectionName = "Customers";
        customers.SourceTableName = "customers";
        return customers;
    }

    public static async Task SeedDiscoveredSchemaAsync(
        IDocumentStore store,
        string slug,
        string? planId = null,
        SelectedSourceTable[]? selectedTables = null,
        CdcSinkConfiguration? mapConfiguration = null,
        CdcSinkSourceTable[]? tables = null)
    {
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new WizardState
        {
            Provider = "Microsoft.Data.SqlClient",
            SourceConnectionString = "Server=shop;Database=shop",
            LastDiscoveredSchema = new CdcSinkSourceSchema
            {
                CatalogName = "shop",
                HasPermissionToSetup = true,
                Tables = [.. tables ?? [SourceTable("orders"), SourceTable("customers"), SourceTable("audit_log")]]
            },
            LastDiscoverAt = DateTime.UtcNow,
            LastMapConfiguration = mapConfiguration,
            PlanId = planId,
            PlanSelectedTables = selectedTables
        }, WizardState.DocumentIdFor(slug));

        await session.SaveChangesAsync();
    }

    public static async Task<WizardState?> LoadStateAsync(IDocumentStore store, string slug)
    {
        using var session = store.OpenAsyncSession();
        return await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(slug));
    }

    public static PlanEntry Entry(CdcSinkTableConfig config, PlanEntryStatus status = PlanEntryStatus.Registered) =>
        new() { Area = "Sales", Version = 1, Rationale = "because", Config = config, Status = status };

    /// <summary>The plan/get answer as the api writes it: PascalCase, nulls written, enums by name, entries keyed by collection.</summary>
    public static string PlanGetBody(params PlanEntry[] entries) =>
        JsonSerializer.Serialize(new
        {
            Plan = new
            {
                Id = PlanId + "/plan",
                Slug = "shop",
                Conventions = new NamingConventions(),
                Entries = entries.ToDictionary(entry => entry.Config!.CollectionName)
            },
            Proposal = new MigrationProposal()
        }, ApiOptions);

    /// <summary>The plan/save answer as the api writes it, on 200 and 422 alike.</summary>
    public static string PlanSaveBody(ValidationIssue[] issues, params string[] warnings) =>
        JsonSerializer.Serialize(new MigrationPlanSaveOutcome(issues, warnings), ApiOptions);

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

    public static DoneFrame Done(string conversationId = PlanId + "/proposal", string planId = PlanId) =>
        new() { ConversationId = conversationId, PlanId = planId };

    public static CdcSinkSourceTable SourceTable(string name, params string[] foreignKeysTo) => new()
    {
        SourceTableSchema = "public",
        SourceTableName = name,
        IsCdcEnabled = true,
        PrimaryKeyColumns = ["order_id"],
        Columns =
        [
            new CdcSinkSourceColumn { Name = "order_id", NativeType = "int", IsPrimaryKey = true, IsCdcCapturable = true },
            new CdcSinkSourceColumn { Name = "ordered_at", NativeType = "timestamp", IsCdcCapturable = true }
        ],
        ForeignKeys = [.. foreignKeysTo.Select(referenced => new CdcSinkSourceForeignKey
        {
            Columns = [$"{referenced}_id"],
            ReferencedSchema = "public",
            ReferencedTable = referenced,
            ReferencedColumns = ["order_id"]
        })]
    };
}

public sealed class StubPlannerHandler(HttpStatusCode status, string body, string contentType = "application/json") : HttpMessageHandler
{
    public string? LastPath { get; private set; }

    public string LastBody { get; private set; } = string.Empty;

    public static StubPlannerHandler Replying(params MigrationFrame[] frames) =>
        new(HttpStatusCode.OK, MigrationSamples.Sse(frames), "text/event-stream");

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        LastPath = request.RequestUri!.AbsolutePath;
        LastBody = await request.Content!.ReadAsStringAsync(cancellationToken);

        return new HttpResponseMessage(status)
        {
            Content = new StringContent(body, Encoding.UTF8, contentType)
        };
    }
}

/// <summary>Holds every planner answer until the test releases it, so turns can be observed while they run.</summary>
public sealed class GatedPlannerHandler(string body) : HttpMessageHandler
{
    private int _arrivals;

    public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public async Task WaitForArrivalsAsync(int count)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (Volatile.Read(ref _arrivals) < count)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Expected {count} planner request(s), saw {_arrivals}.");

            await Task.Delay(25);
        }
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _arrivals);
        await Release.Task.WaitAsync(cancellationToken);

        return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "text/event-stream") };
    }
}

/// <summary>Holds the consent check until the test releases it, so the start of a turn can be observed.</summary>
public sealed class GatedConsentClient : IAiHelperClient
{
    public TaskCompletionSource Reached { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public async Task<AiHelperStatus> CheckConsentAsync(CancellationToken ct)
    {
        Reached.TrySetResult();
        await Release.Task.WaitAsync(ct);
        return AiHelperStatus.Success;
    }

    public Task<AiHelperStatus> GiveConsentAsync(CancellationToken ct) => Task.FromResult(AiHelperStatus.Success);

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

public sealed class ThrowingPlannerHandler(Exception failure) : HttpMessageHandler
{
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        Task.FromException<HttpResponseMessage>(failure);
}

/// <summary>Sends <paramref name="before"/>, then never another byte and never closes: a stalled api.</summary>
public sealed class HangingStreamPlannerHandler(string before) : HttpMessageHandler
{
    /// <summary>Set once everything in <c>before</c> was read and the stream hangs.</summary>
    public TaskCompletionSource Hanging { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StreamContent(new HangingStream(Encoding.UTF8.GetBytes(before), Hanging))
        });

    private sealed class HangingStream(byte[] before, TaskCompletionSource hanging) : MemoryStream(before)
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (Position < Length)
                return await base.ReadAsync(buffer, cancellationToken);

            hanging.TrySetResult();
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return 0;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    }
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
