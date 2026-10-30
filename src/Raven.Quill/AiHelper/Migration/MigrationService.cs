using System.IO.Compression;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper.Migration.Planning;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Logging;
using Raven.Quill.Wizard;

namespace Raven.Quill.AiHelper.Migration;

/// <summary>
/// The Quill half of a planning session: which app it belongs to, which schema it reads, whether the operator
/// has consented, and what to do with the plan once they are happy with it. The api owns the plan; Quill keeps
/// its id per app and relays every turn, read and save.
/// </summary>
public sealed class MigrationService(
    HttpClient httpClient,
    IDocumentStore store,
    IAiHelperClient aiClient,
    MigrationTurns turns,
    QuillLogger<MigrationService> logger)
{
    private const string DataPrefix = "data: ";

    private const string ServiceUnreachable = "The AI service could not be reached.";

    /// <summary>
    /// The opening message from the design: the agent groups the tables and explains itself, and
    /// registers nothing until the operator has chosen.
    /// </summary>
    public const string DefaultStartPrompt =
        "Look at the set of tables, propose key areas to migrate, the specific collections to create " +
        "(and the tables they include). Explain briefly the reasoning and what sort of agents and " +
        "behaviours this allows.";

    /// <summary>
    /// Why a request was turned away. <see cref="Status"/> is set only when the AI service is the
    /// reason, so the endpoint can tell "you have not consented" apart from "the service is down".
    /// </summary>
    public sealed record Refusal(string Message, AiHelperStatus? Status = null, bool IsConflict = false);

    private static readonly Refusal NoPlanYet = new("no plan yet; call /api/setup/migration/proposal/start first");

    private static readonly Refusal UnreadableAnswer = new("the AI service sent an answer that could not be read", AiHelperStatus.InternalError);

    private static readonly JsonSerializerOptions ApiJson = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public async Task<Refusal?> ProposalStartAsync(MigrationProposalStartRequest request, MigrationStream browserStream, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.Slug))
            return new Refusal("slug is required");

        var (schema, refusal) = await LoadSchemaAsync(request.Slug, request.SelectedTables, token);
        if (refusal is not null)
            return refusal;

        var prompt = string.IsNullOrWhiteSpace(request.Prompt) ? DefaultStartPrompt : request.Prompt!;
        var turn = new TurnParameters
        {
            Path = "/assistant/migration/proposal/start",
            Slug = request.Slug,
            Agent = PlannerAgent.Proposal,
            Opening = request,
            PlannerRequest = JsonSerializer.Serialize(new PlannerStartRequest { Slug = request.Slug, SchemaGzip = GzipSchema(schema!), Prompt = prompt }, ApiJson)
        };

        return await TurnAsync(turn, browserStream, token);
    }

    public async Task<Refusal?> ProposalAskAsync(MigrationProposalAskRequest request, MigrationStream browserStream, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.Slug))
            return new Refusal("slug is required");
        if (string.IsNullOrWhiteSpace(request.Prompt))
            return new Refusal("prompt is required");

        var (planId, refusal) = await LoadPlanIdAsync(request.Slug, token);
        if (refusal is not null)
            return refusal;

        var turn = new TurnParameters
        {
            Path = "/assistant/migration/proposal/ask",
            Slug = request.Slug,
            Agent = PlannerAgent.Proposal,
            PlannerRequest = JsonSerializer.Serialize(new PlannerAskRequest { PlanId = planId, Prompt = request.Prompt }, ApiJson)
        };

        return await TurnAsync(turn, browserStream, token);
    }

    public async Task<Refusal?> AreaStartAsync(MigrationAreaStartRequest request, MigrationStream browserStream, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.Slug))
            return new Refusal("slug is required");
        if (string.IsNullOrWhiteSpace(request.Area))
            return new Refusal("area is required");

        var (planId, refusal) = await LoadPlanIdAsync(request.Slug, token);
        if (refusal is not null)
            return refusal;

        var turn = new TurnParameters
        {
            Path = "/assistant/migration/area/start",
            Slug = request.Slug,
            Agent = PlannerAgent.Area,
            Area = request.Area,
            PlannerRequest = JsonSerializer.Serialize(new PlannerAskRequest { PlanId = planId, Area = request.Area }, ApiJson)
        };

        return await TurnAsync(turn, browserStream, token);
    }

    public async Task<Refusal?> AreaAskAsync(MigrationAreaAskRequest request, MigrationStream browserStream, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.Slug))
            return new Refusal("slug is required");
        if (string.IsNullOrWhiteSpace(request.Area))
            return new Refusal("area is required");
        if (string.IsNullOrWhiteSpace(request.Prompt))
            return new Refusal("prompt is required");

        var (planId, refusal) = await LoadPlanIdAsync(request.Slug, token);
        if (refusal is not null)
            return refusal;

        var turn = new TurnParameters
        {
            Path = "/assistant/migration/area/ask",
            Slug = request.Slug,
            Agent = PlannerAgent.Area,
            Area = request.Area,
            PlannerRequest = JsonSerializer.Serialize(new PlannerAskRequest { PlanId = planId, Area = request.Area, Prompt = request.Prompt }, ApiJson)
        };

        return await TurnAsync(turn, browserStream, token);
    }

    private async Task<Refusal?> TurnAsync(TurnParameters turn, MigrationStream browserStream, CancellationToken token)
    {
        using var running = turns.TryBeginTurn(turn.Slug, turn.Agent, turn.Area);
        if (running.Refusal is not null)
            return running.Refusal;

        var consent = await RequireConsentAsync(token);
        if (consent is not null)
            return consent;

        await RelayAsync(turn, browserStream, running);
        return null;
    }

    public Refusal? Stop(MigrationStopRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Slug))
            return new Refusal("slug is required");
        if (Enum.IsDefined(request.Agent) == false)
            return new Refusal("agent must be Proposal or Area");

        var hasArea = string.IsNullOrWhiteSpace(request.Area) == false;
        if (request.Agent == PlannerAgent.Area && hasArea == false)
            return new Refusal("area is required to stop an area turn");
        if (request.Agent == PlannerAgent.Proposal && hasArea)
            return new Refusal("a proposal turn has no area");

        turns.Stop(request.Slug, request.Agent, request.Area);
        return null;
    }

    public async Task<(MigrationPlanGetResponse? Response, Refusal? Refusal)> PlanGetAsync(MigrationPlanGetRequest request, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.Slug))
            return (null, new Refusal("slug is required"));

        var (planId, refusal) = await LoadPlanIdAsync(request.Slug, token);
        if (refusal is not null)
            return (null, refusal);

        return await ReadPlanAsync(planId!, token);
    }

    public async Task<(MigrationPlanSaveOutcome? Outcome, Refusal? Refusal)> PlanSaveAsync(MigrationPlanSaveRequest request, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.Slug))
            return (null, new Refusal("slug is required"));
        if (request.Changes is not { Length: > 0 })
            return (null, new Refusal("changes are required"));

        using var save = turns.TryBeginSave(request.Slug);
        if (save.Refusal is not null)
            return (null, save.Refusal);

        var (planId, refusal) = await LoadPlanIdAsync(request.Slug, token);
        if (refusal is not null)
            return (null, refusal);

        return await ReadAsync<MigrationPlanSaveOutcome>("/assistant/migration/plan/save", new PlannerPlanRequest { PlanId = planId, Changes = request.Changes }, token);
    }

    /// <summary>
    /// Turns the registered collections of the api's plan into the configuration the wizard carries on with.
    /// The per-call validation the model saw is not enough on its own: only the assembled configuration can be
    /// checked for names colliding across a table's own mappings, and for join columns that name a mapped
    /// property instead of a source column.
    /// </summary>
    public async Task<(MigrationApplyResponse? Response, Refusal? Refusal)> ApplyAsync(MigrationApplyRequest request, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.Slug))
            return (null, new Refusal("slug is required"));

        using var session = store.OpenAsyncSession();
        var state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(request.Slug), token);
        if (state?.PlanId is null)
            return (null, NoPlanYet);
        if (state.LastDiscoveredSchema is null)
            return (null, new Refusal("no discovered schema found; call /api/setup/discover first"));

        var (plan, refusal) = await ReadPlanAsync(state.PlanId, token);
        if (refusal is not null)
            return (null, refusal);

        var registered = plan!.Plan.Entries.Values.Where(entry => entry.Status == PlanEntryStatus.Registered).ToArray();
        if (registered.Length == 0)
            return (null, new Refusal("the plan has no collections yet"));

        var configuration = PlanToCdcConfiguration.Build(registered, state.LastMapConfiguration);

        if (configuration.Validate(out var errors, validateName: false, validateConnection: false) == false)
            return (new MigrationApplyResponse(null, [], errors.ToArray()), null);

        PlanToCdcConfiguration.ValidateJoinColumnsAgainstSchema(configuration, state.LastDiscoveredSchema, errors);
        if (errors.Count > 0)
            return (new MigrationApplyResponse(null, [], errors.ToArray()), null);

        var coverageSchema = state.PlanSelectedTables is { Length: > 0 }
            ? WizardEndpoints.SelectTables(state.LastDiscoveredSchema, state.PlanSelectedTables)
            : state.LastDiscoveredSchema;

        var defaultSchema = CdcSinkDefaultSchema.Resolve(state.Provider!, state.SourceConnectionString!);
        var unmapped = PlanToCdcConfiguration.UnmappedTables(configuration, coverageSchema, defaultSchema);

        state.LastMapConfiguration = configuration;
        state.LastMapAt = DateTime.UtcNow;
        state.AppliedPlanVersions = plan.Plan.Entries
            .Where(pair => pair.Value.Status == PlanEntryStatus.Registered)
            .ToDictionary(pair => pair.Key, pair => pair.Value.Version);
        await session.SaveChangesAsync(token);

        return (new MigrationApplyResponse(configuration, unmapped, []), null);
    }

    /// <summary>
    /// Reads the upstream stream to its end on its own, whether or not the browser is still there: a turn the
    /// api started finishes, unless the operator stops it, which cancels the upstream request and with it the
    /// api's turn. The lock is released at the done frame. The opening turn stores the plan id from its
    /// done frame, or from an error frame that names the plan a failed start left, before the browser sees it and
    /// asks for the plan. The client timeout only covers the headers of a streamed response, so the read keeps
    /// its own clock, rewound at every line: a turn may take as long as it needs while the api keeps talking, and
    /// a stalled one is cut after one silent timeout.
    /// </summary>
    private async Task RelayAsync(TurnParameters turn, MigrationStream browserStream, MigrationTurns.Turn running)
    {
        using var silence = CancellationTokenSource.CreateLinkedTokenSource(running.Stopped);
        silence.CancelAfter(httpClient.Timeout);
        HttpResponseMessage response;

        try
        {
            using var content = new StringContent(turn.PlannerRequest, Encoding.UTF8, "application/json");
            using var httpRequest = new HttpRequestMessage(HttpMethod.Post, turn.Path) { Content = content };
            response = await httpClient.SendAsync(httpRequest, HttpCompletionOption.ResponseHeadersRead, silence.Token);
        }
        catch (Exception e) when (IsTransportFailure(e))
        {
            if (running.Stopped.IsCancellationRequested == false && logger.IsWarnEnabled)
                logger.Warn(e, $"Planner {turn.Path} failed (transport).");

            await browserStream.WriteAsync(Interrupted(running));
            return;
        }

        using (response)
        {
            if (response.IsSuccessStatusCode == false)
            {
                var failureBody = await ReadFailureBodyAsync(response, silence.Token);

                if (logger.IsInfoEnabled)
                    logger.Info($"Planner {turn.Path} failed: upstream {(int)response.StatusCode}. Body: {failureBody}");

                await browserStream.WriteAsync(new ErrorFrame { Message = DescribeFailure(response.StatusCode, failureBody) });
                return;
            }

            await using var apiBody = await response.Content.ReadAsStreamAsync(silence.Token);
            using var apiReader = new StreamReader(apiBody, Encoding.UTF8);

            MigrationFrame? frame = null;

            try
            {
                while (await apiReader.ReadLineAsync(silence.Token) is { } line)
                {
                    silence.CancelAfter(httpClient.Timeout);

                    if (line.StartsWith(DataPrefix, StringComparison.Ordinal) == false)
                        continue;

                    if (ReadFrame(turn.Path, line[DataPrefix.Length..]) is not { } parsed)
                        continue;

                    frame = parsed;
                    var planId = frame switch
                    {
                        DoneFrame done => done.PlanId,
                        ErrorFrame error => error.PlanId,
                        _ => null
                    };
                    if (turn.Opening is not null && planId is not null)
                        await StorePlanAsync(turn.Opening.Slug, planId, turn.Opening.SelectedTables);

                    if (frame is DoneFrame)
                        running.Dispose();

                    await browserStream.WriteAsync(frame);
                }

                if (frame is not (DoneFrame or ErrorFrame))
                {
                    if (logger.IsWarnEnabled)
                        logger.Warn($"Planner {turn.Path} stream ended without a done or error frame (last frame: {frame?.GetType().Name ?? "none"}).");

                    await browserStream.WriteAsync(new ErrorFrame { Message = "The AI service stopped answering before the turn finished." });
                }
            }
            catch (Exception e) when (IsTransportFailure(e))
            {
                if (running.Stopped.IsCancellationRequested == false && logger.IsWarnEnabled)
                    logger.Warn(e, $"Planner {turn.Path} stream broke.");

                if (frame is not (DoneFrame or ErrorFrame))
                    await browserStream.WriteAsync(Interrupted(running));
            }
        }
    }

    private static ErrorFrame Interrupted(MigrationTurns.Turn running) =>
        new() { Message = running.Stopped.IsCancellationRequested ? "The turn was stopped." : ServiceUnreachable };

    private async Task StorePlanAsync(string slug, string planId, SelectedSourceTable[]? selectedTables)
    {
        using var session = store.OpenAsyncSession();
        var id = WizardState.DocumentIdFor(slug);
        session.Advanced.Patch<WizardState, string?>(id, state => state.PlanId, planId);
        session.Advanced.Patch<WizardState, SelectedSourceTable[]?>(id, state => state.PlanSelectedTables, selectedTables);
        await session.SaveChangesAsync();
    }

    private async Task<(MigrationPlanGetResponse? Plan, Refusal? Refusal)> ReadPlanAsync(string planId, CancellationToken token)
    {
        var (plan, refusal) = await ReadAsync<MigrationPlanGetResponse>("/assistant/migration/plan/get", new PlannerPlanRequest { PlanId = planId }, token);
        if (refusal is null && plan!.Plan is null)
            return (null, UnreadableAnswer);

        return (plan, refusal);
    }

    /// <summary>A 422 is an answer too: it carries what the api refused.</summary>
    private async Task<(T? Answer, Refusal? Refusal)> ReadAsync<T>(string path, PlannerPlanRequest request, CancellationToken token) where T : class
    {
        var (response, refusal) = await PostAsync(path, JsonSerializer.Serialize(request, ApiJson), token);
        if (refusal is not null)
            return (null, refusal);

        using (response)
        {
            var body = await response!.Content.ReadAsStringAsync(token);
            if (response.IsSuccessStatusCode == false && response.StatusCode != HttpStatusCode.UnprocessableEntity)
                return (null, new Refusal(DescribeFailure(response.StatusCode, body), (int)response.StatusCode switch
                {
                    401 => AiHelperStatus.ConsentRequired,
                    >= 500 => AiHelperStatus.InternalError,
                    _ => null
                }));

            try
            {
                return JsonSerializer.Deserialize<T>(body, ApiJson) is { } answer ? (answer, null) : (null, UnreadableAnswer);
            }
            catch (JsonException e)
            {
                if (logger.IsWarnEnabled)
                    logger.Warn(e, $"Planner {path} sent an answer that could not be read.");

                return (null, UnreadableAnswer);
            }
        }
    }

    private async Task<(HttpResponseMessage? Response, Refusal? Refusal)> PostAsync(string path, string json, CancellationToken token)
    {
        try
        {
            using var content = new StringContent(json, Encoding.UTF8, "application/json");
            return (await httpClient.PostAsync(path, content, token), null);
        }
        catch (Exception e) when (IsTransportFailure(e, token))
        {
            if (logger.IsWarnEnabled)
                logger.Warn(e, $"Planner {path} failed (transport).");

            return (null, new Refusal(ServiceUnreachable, AiHelperStatus.InternalError));
        }
    }

    // Without a token, a cancellation can only be the client's own timeout, so it counts as a failure too.
    private static bool IsTransportFailure(Exception e, CancellationToken token = default) =>
        e is HttpRequestException or IOException ||
        (e is OperationCanceledException && token.IsCancellationRequested == false);

    private MigrationFrame? ReadFrame(string path, string json)
    {
        try
        {
            return JsonSerializer.Deserialize<MigrationFrame>(json, NdjsonStream.JsonOpts);
        }
        catch (JsonException e)
        {
            if (logger.IsWarnEnabled)
                logger.Warn(e, $"Planner {path} sent a frame that could not be read; skipping it.");

            return null;
        }
    }

    private static string DescribeFailure(HttpStatusCode statusCode, string body) => statusCode switch
    {
        HttpStatusCode.Unauthorized => "The AI service refused the request: consent is required or the license was not accepted.",
        HttpStatusCode.TooManyRequests when IsOutOfTokens(body) => "The monthly AI token quota is used up.",
        HttpStatusCode.TooManyRequests => "The AI service is busy. Try again in a moment.",
        HttpStatusCode.NotFound => "No plan found for this app.",
        HttpStatusCode.BadRequest => "The AI service rejected the planning request.",
        HttpStatusCode.RequestEntityTooLarge => "The selected schema is too large for the AI service. Select fewer tables.",
        _ => $"The AI service failed (HTTP {(int)statusCode})."
    };

    private static async Task<string> ReadFailureBodyAsync(HttpResponseMessage response, CancellationToken token)
    {
        try
        {
            return await response.Content.ReadAsStringAsync(token);
        }
        catch (Exception e) when (IsTransportFailure(e))
        {
            return string.Empty;
        }
    }

    private static bool IsOutOfTokens(string body)
    {
        try
        {
            using var document = JsonDocument.Parse(body);
            return document.RootElement.ValueKind == JsonValueKind.Object &&
                   document.RootElement.TryGetProperty("Status", out var status) &&
                   status.ValueKind == JsonValueKind.String &&
                   string.Equals(status.GetString(), nameof(AiHelperStatus.OutOfTokens), StringComparison.OrdinalIgnoreCase);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static byte[] GzipSchema(CdcSinkSourceSchema schema)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(schema, ApiJson);
        using var buffer = new MemoryStream();
        using (var gzip = new GZipStream(buffer, CompressionLevel.Optimal, leaveOpen: true))
            gzip.Write(json);
        return buffer.ToArray();
    }

    /// <summary>
    /// The discovered schema, narrowed to the tables the operator selected. Discovery enumerates whole
    /// schemas, so handing the AI everything it found would map tables the operator deliberately left out.
    /// </summary>
    private async Task<(CdcSinkSourceSchema? Schema, Refusal? Refusal)> LoadSchemaAsync(string slug, SelectedSourceTable[]? selected, CancellationToken token)
    {
        WizardState? state;
        using (var session = store.OpenAsyncSession())
            state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(slug), token);

        if (state?.LastDiscoveredSchema is null)
            return (null, new Refusal("no discovered schema found; call /api/setup/discover first"));

        if (selected is not { Length: > 0 })
            return (state.LastDiscoveredSchema, null);

        var narrowed = WizardEndpoints.SelectTables(state.LastDiscoveredSchema, selected);

        return narrowed.Tables.Count == 0
            ? (null, new Refusal("none of the selected tables are part of the discovered schema"))
            : (narrowed, null);
    }

    private async Task<(string? PlanId, Refusal? Refusal)> LoadPlanIdAsync(string slug, CancellationToken token)
    {
        using var session = store.OpenAsyncSession();
        var state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(slug), token);
        return state?.PlanId is null ? (null, NoPlanYet) : (state.PlanId, null);
    }

    /// <summary>The same gate the one-shot path runs.</summary>
    private async Task<Refusal?> RequireConsentAsync(CancellationToken token)
    {
        var status = await aiClient.CheckConsentAsync(token);

        if (status == AiHelperStatus.Success)
            return null;

        return status switch
        {
            AiHelperStatus.ConsentRequired => new Refusal("consent to the RavenDB AI service is required", status),
            AiHelperStatus.InvalidCredentials => new Refusal("the RavenDB AI service did not accept the license", status),
            _ => new Refusal(ServiceUnreachable, status)
        };
    }

    private sealed class TurnParameters
    {
        public required string Path { get; init; }

        public required string Slug { get; init; }

        public required PlannerAgent Agent { get; init; }

        public string? Area { get; init; }

        public MigrationProposalStartRequest? Opening { get; init; }

        public required string PlannerRequest { get; init; }
    }

    private sealed class PlannerStartRequest
    {
        public string? Slug { get; init; }

        public byte[]? SchemaGzip { get; init; }

        public string? Prompt { get; init; }
    }

    private sealed class PlannerAskRequest
    {
        public string? PlanId { get; init; }

        public string? Area { get; init; }

        public string? Prompt { get; init; }
    }

    private sealed class PlannerPlanRequest
    {
        public string? PlanId { get; init; }

        public MigrationPlanChange[]? Changes { get; init; }
    }
}
