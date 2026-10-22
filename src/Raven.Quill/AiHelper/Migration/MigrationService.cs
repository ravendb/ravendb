using System.IO.Compression;
using System.Net;
using System.Text;
using System.Text.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper.Migration.Planning;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Logging;
using Raven.Quill.Wizard;
using Sparrow.Json;

namespace Raven.Quill.AiHelper.Migration;

/// <summary>
/// The Quill half of a planning session: which app it belongs to, which schema it reads, whether
/// the operator has consented, and what to do with the plan once they are happy with it.
/// </summary>
public sealed class MigrationService(
    HttpClient httpClient,
    IDocumentStore store,
    IAiHelperClient aiClient,
    MigrationTurns turns,
    QuillLogger<MigrationService> logger)
{
    public const string StartPath = "/assistant/migration/start";

    public const string AskPath = "/assistant/migration/ask";

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

    private static readonly Refusal TurnAlreadyRunning = new("a planner turn is already running for this app", IsConflict: true);

    public async Task<Refusal?> StartAsync(
        MigrationStartRequest request,
        Func<MigrationFrame, Task> onFrame,
        CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.Slug))
            return new Refusal("slug is required");

        using var turn = await turns.TryBeginTurnAsync(request.Slug, token);
        if (turn is null)
            return TurnAlreadyRunning;

        var (_, schema, refusal) = await LoadWizardAsync(request.Slug, request.SelectedTables, token);
        if (refusal is not null)
            return refusal;

        var consent = await RequireConsentAsync(token);
        if (consent is not null)
            return consent;

        var prompt = string.IsNullOrWhiteSpace(request.Prompt) ? DefaultStartPrompt : request.Prompt!;

        await RelayAsync(
            StartPath,
            new PlannerStartRequest { Slug = request.Slug, SchemaGzip = GzipSchema(schema!), Prompt = prompt },
            request.Slug,
            conversationId: null,
            new MigrationPlan { SelectedTables = request.SelectedTables },
            onFrame,
            turn,
            token);

        return null;
    }

    public async Task<Refusal?> AskAsync(
        MigrationAskRequest request,
        Func<MigrationFrame, Task> onFrame,
        CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.ConversationId))
            return new Refusal("conversationId is required");
        if (string.IsNullOrWhiteSpace(request.Prompt) && HasSelection(request) == false)
            return new Refusal("a prompt or a selection is required");
        if (string.IsNullOrWhiteSpace(request.Slug))
            return new Refusal("slug is required");

        using var turn = await turns.TryBeginTurnAsync(request.Slug, token);
        if (turn is null)
            return TurnAlreadyRunning;

        var (state, _, refusal) = await LoadWizardAsync(request.Slug, selected: null, token);
        if (refusal is not null)
            return refusal;

        // Every session persists its plan at the end of its opening turn, so nothing found here means
        // the conversation is not this app's to continue, whether or not it exists at all.
        var plan = CurrentPlan(state!, request.ConversationId);
        if (plan is null)
            return new Refusal("no planning session found for that conversation");

        var consent = await RequireConsentAsync(token);
        if (consent is not null)
            return consent;

        await RelayAsync(
            AskPath,
            new PlannerAskRequest
            {
                Slug = request.Slug,
                ConversationId = request.ConversationId,
                Prompt = request.Prompt,
                SelectedAreas = request.SelectedAreas,
                SelectedCollections = request.SelectedCollections,
                Plan = new PlannerPlan { Conventions = plan.Conventions, Entries = plan.CurrentEntries() },
                UserEdits = plan.PendingUserEdits.ToList()
            },
            request.Slug,
            request.ConversationId,
            plan,
            onFrame,
            turn,
            token);

        return null;
    }

    /// <summary>
    /// How an operator's change ended: refused, refused because a turn is running, rejected with the errors
    /// CDC Sink would raise, or applied with <see cref="Warnings"/>.
    /// </summary>
    public sealed record EditOutcome(Refusal? Refusal = null, bool TurnRunning = false, string[]? Errors = null, string[]? Warnings = null);

    /// <summary>
    /// Applies the operator's change to one collection. The checks are the ones apply runs on the assembled
    /// configuration - what CDC Sink would refuse, and join columns the source tables lack - so an edit the
    /// plan accepts cannot fail there later. Whether it fits the planner's own rules is left to the planner.
    /// </summary>
    public async Task<EditOutcome> EditCollectionAsync(MigrationEditCollectionRequest request, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.ConversationId))
            return new EditOutcome(new Refusal("conversationId is required"));
        if (string.IsNullOrWhiteSpace(request.Collection))
            return new EditOutcome(new Refusal("collection is required"));
        if (string.IsNullOrWhiteSpace(request.Slug))
            return new EditOutcome(new Refusal("slug is required"));

        using var edit = await turns.TryBeginEditAsync(request.Slug, token);
        if (edit is null)
            return new EditOutcome(TurnRunning: true);

        WizardState? state;
        using (var session = store.OpenAsyncSession())
            state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(request.Slug), token);

        var plan = state is null ? null : CurrentPlan(state, request.ConversationId);
        if (plan is null)
            return new EditOutcome(new Refusal("no plan found for that conversation"));

        if (request.Config is not null)
        {
            if (string.IsNullOrWhiteSpace(request.Config.CollectionName))
                request.Config.CollectionName = request.Collection;

            var errors = ValidateEdit(request.Collection, request.Config, state!.LastDiscoveredSchema);
            if (errors.Length > 0)
                return new EditOutcome(Errors: errors);
        }

        var existed = plan.CurrentEntries().Any(entry => string.Equals(entry.Collection, request.Collection, StringComparison.OrdinalIgnoreCase));
        var refused = plan.ApplyUserEdit(request.Collection, request.Config);
        if (refused is not null)
            return new EditOutcome(new Refusal(refused));

        await plan.SaveAsync(store, request.Slug, token);

        return new EditOutcome(Warnings: existed ? DanglingLinks(plan, request.Collection, request.Config) : []);
    }

    private static string[] ValidateEdit(string collection, CdcSinkTableConfig config, CdcSinkSourceSchema? schema)
    {
        var entry = new PlanEntry { Collection = collection, Config = config };
        var configuration = PlanToCdcConfiguration.Build([entry], previousMapping: null);

        if (configuration.Validate(out var errors, validateName: false, validateConnection: false) == false)
            return errors.ToArray();

        if (schema is not null)
            WizardEndpoints.ValidateJoinColumnsAgainstSchema(configuration, schema, errors);

        return errors.ToArray();
    }

    private static string[] DanglingLinks(MigrationPlan plan, string collection, CdcSinkTableConfig? config)
    {
        var renamedTo = config is null || string.Equals(config.CollectionName, collection, StringComparison.OrdinalIgnoreCase)
            ? null
            : config.CollectionName;

        if (config is not null && renamedTo is null)
            return [];

        return plan.LinkingTo(collection)
            .Select(linking => renamedTo is null
                ? $"{linking} links to {collection}, which is no longer in the plan. Update or remove that link."
                : $"{linking} links to {collection}, which is now named {renamedTo}. Update that link.")
            .ToArray();
    }

    /// <summary>
    /// Turns the registered plan into the configuration the wizard carries on with. The per-call
    /// validation the model saw is not enough on its own: only the assembled configuration can be
    /// checked for names colliding across a table's own mappings, and for join columns that name a
    /// mapped property instead of a source column.
    /// </summary>
    public async Task<(MigrationApplyResponse? Response, Refusal? Refusal)> ApplyAsync(
        MigrationApplyRequest request,
        CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.ConversationId))
            return (null, new Refusal("conversationId is required"));

        using var session = store.OpenAsyncSession();
        var state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(request.Slug), token);

        var plan = state is null ? null : CurrentPlan(state, request.ConversationId);
        if (plan is null)
            return (null, new Refusal("no plan found for that conversation"));

        if (plan.Entries.Count == 0)
            return (null, new Refusal("the plan has no collections yet"));

        var (selected, unknown) = SelectCollections(plan.Entries, request.Collections);

        if (unknown.Length > 0)
            return (null, new Refusal($"the plan has no collection named {string.Join(", ", unknown)}"));

        if (selected.Length == 0)
            return (null, new Refusal("no collections were selected"));

        if (state!.LastDiscoveredSchema is null)
            return (null, new Refusal("no discovered schema found; call /api/setup/discover first"));

        var configuration = PlanToCdcConfiguration.Build(selected, state.LastMapConfiguration);

        if (configuration.Validate(out var errors, validateName: false, validateConnection: false) == false)
            return (new MigrationApplyResponse(null, [], errors.ToArray()), null);

        WizardEndpoints.ValidateJoinColumnsAgainstSchema(configuration, state.LastDiscoveredSchema, errors);
        if (errors.Count > 0)
            return (new MigrationApplyResponse(null, [], errors.ToArray()), null);

        var coverageSchema = plan.SelectedTables is { Length: > 0 }
            ? WizardEndpoints.SelectTables(state.LastDiscoveredSchema, plan.SelectedTables)
            : state.LastDiscoveredSchema;

        var defaultSchema = CdcSinkDefaultSchema.Resolve(state.Provider!, state.SourceConnectionString!);
        var unmapped = PlanToCdcConfiguration.UnmappedTables(configuration, coverageSchema, defaultSchema);

        state.LastMapConfiguration = configuration;
        state.LastMapAt = DateTime.UtcNow;
        await session.SaveChangesAsync(token);

        return (new MigrationApplyResponse(configuration, unmapped, []), null);
    }

    private static MigrationPlan? CurrentPlan(WizardState state, string conversationId) =>
        state.MigrationPlan is { } plan && string.Equals(plan.ConversationId, conversationId, StringComparison.Ordinal)
            ? plan
            : null;

    private async Task RelayAsync(
        string path,
        object request,
        string slug,
        string? conversationId,
        MigrationPlan plan,
        Func<MigrationFrame, Task> onFrame,
        IDisposable turn,
        CancellationToken token)
    {
        HttpResponseMessage response;

        try
        {
            using var content = new StringContent(SerializeRequest(request), Encoding.UTF8, "application/json");
            using var httpRequest = new HttpRequestMessage(HttpMethod.Post, path) { Content = content };
            response = await httpClient.SendAsync(httpRequest, HttpCompletionOption.ResponseHeadersRead, token);
        }
        catch (Exception e) when (IsTransportFailure(e, token))
        {
            if (logger.IsWarnEnabled)
                logger.Warn(e, $"Planner {path} failed (transport).");

            await onFrame(new ErrorFrame { Message = ServiceUnreachable });
            return;
        }

        using (response)
        {
            if (response.IsSuccessStatusCode == false)
            {
                var tooManyRequests = response.StatusCode == HttpStatusCode.TooManyRequests;
                var failureBody = tooManyRequests ? await ReadFailureBodyAsync(response, token) : string.Empty;

                if (logger.IsInfoEnabled)
                    logger.Info(tooManyRequests
                        ? $"Planner {path} failed: upstream 429. Body: {failureBody}"
                        : $"Planner {path} failed: upstream {(int)response.StatusCode}.");

                await onFrame(new ErrorFrame { Message = DescribeFailure(response.StatusCode, failureBody) });
                return;
            }

            await using var body = await response.Content.ReadAsStreamAsync(token);
            using var reader = new StreamReader(body, Encoding.UTF8);

            MigrationFrame? frame = null;

            try
            {
                while (await reader.ReadLineAsync(token) is { } line)
                {
                    if (line.StartsWith(DataPrefix, StringComparison.Ordinal) == false)
                        continue;

                    frame = ReadFrame(path, line[DataPrefix.Length..]);
                    if (frame is null)
                        continue;

                    if (frame is DoneFrame done)
                    {
                        conversationId = done.ConversationId;
                        plan.PendingUserEdits.Clear();
                    }

                    if (Mirror(plan, frame) && conversationId is not null)
                    {
                        plan.ConversationId = conversationId;
                        await plan.SaveAsync(store, slug, token);
                    }

                    if (frame is DoneFrame)
                        turn.Dispose();

                    await onFrame(frame);
                }

                if (frame is not (DoneFrame or ErrorFrame) && logger.IsWarnEnabled)
                    logger.Warn($"Planner {path} stream ended without a done or error frame (last frame: {frame?.GetType().Name ?? "none"}).");
            }
            catch (Exception e) when (IsTransportFailure(e, token))
            {
                if (logger.IsWarnEnabled)
                    logger.Warn(e, $"Planner {path} stream broke.");

                await onFrame(new ErrorFrame { Message = ServiceUnreachable });
            }
        }
    }

    private static bool IsTransportFailure(Exception e, CancellationToken token) =>
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

    private static bool Mirror(MigrationPlan plan, MigrationFrame frame)
    {
        switch (frame)
        {
            case CollectionFrame collection:
                plan.Upsert(collection.Collection, collection.Rationale, collection.Config);
                plan.ForgetUserEdits(collection.Collection, includingRemoval: true);
                return true;

            case RemovedFrame { Collection: not null } removed:
                plan.Remove(removed.Collection);
                plan.ForgetUserEdits(removed.Collection, includingRemoval: false);
                return true;

            case ConventionsFrame conventions:
                plan.SetConventions(new NamingConventions(conventions.PropertyCase, conventions.PropertyLanguage, conventions.Notes));
                return true;

            case DoneFrame:
                return true;

            default:
                return false;
        }
    }

    private static string DescribeFailure(HttpStatusCode statusCode, string body) => statusCode switch
    {
        HttpStatusCode.Unauthorized => "The AI service refused the request: consent is required or the license was not accepted.",
        HttpStatusCode.TooManyRequests when IsOutOfTokens(body) => "The monthly AI token quota is used up.",
        HttpStatusCode.TooManyRequests => "The AI service is busy. Try again in a moment.",
        HttpStatusCode.NotFound => "No planning session found for that conversation.",
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
        catch (Exception e) when (IsTransportFailure(e, token))
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

    private string SerializeRequest(object request)
    {
        using var ctx = JsonOperationContext.ShortTermSingleUse();
        return store.Conventions.Serialization.DefaultConverter.ToBlittable(request, ctx).ToString();
    }

    private byte[] GzipSchema(CdcSinkSourceSchema schema)
    {
        var json = Encoding.UTF8.GetBytes(SerializeRequest(schema));
        using var buffer = new MemoryStream();
        using (var gzip = new GZipStream(buffer, CompressionLevel.Optimal, leaveOpen: true))
            gzip.Write(json);
        return buffer.ToArray();
    }

    /// <summary>
    /// Narrows the plan to the collections the operator kept. Naming one the plan does not hold is
    /// a mistake worth reporting rather than quietly ignoring - it usually means the caller is
    /// working from a stale view of the plan.
    /// </summary>
    private static (PlanEntry[] Selected, string[] Unknown) SelectCollections(
        IReadOnlyCollection<PlanEntry> entries,
        string[]? requested)
    {
        if (requested is not { Length: > 0 })
            return (entries.ToArray(), []);

        var wanted = requested.ToHashSet(StringComparer.OrdinalIgnoreCase);

        var unknown = wanted
            .Where(name => entries.Any(e => string.Equals(e.Collection, name, StringComparison.OrdinalIgnoreCase)) == false)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        return (entries.Where(e => wanted.Contains(e.Collection)).ToArray(), unknown);
    }

    private async Task<(WizardState? State, CdcSinkSourceSchema? Schema, Refusal? Refusal)> LoadWizardAsync(
        string slug,
        SelectedSourceTable[]? selected,
        CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(slug))
            return (null, null, new Refusal("slug is required"));

        WizardState? state;
        using (var session = store.OpenAsyncSession())
            state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(slug), token);

        if (state?.LastDiscoveredSchema is null)
            return (null, null, new Refusal("no discovered schema found; call /api/setup/discover first"));

        if (selected is not { Length: > 0 })
            return (state, state.LastDiscoveredSchema, null);

        var narrowed = WizardEndpoints.SelectTables(state.LastDiscoveredSchema, selected);

        return narrowed.Tables.Count == 0
            ? (null, null, new Refusal("none of the selected tables are part of the discovered schema"))
            : (state, narrowed, null);
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

    private sealed class PlannerStartRequest
    {
        public string? Slug { get; init; }

        public byte[]? SchemaGzip { get; init; }

        public string? Prompt { get; init; }
    }

    private static bool HasSelection(MigrationAskRequest request) =>
        (request.SelectedAreas ?? []).Concat(request.SelectedCollections ?? []).Any(name => string.IsNullOrWhiteSpace(name) == false);

    private sealed class PlannerAskRequest
    {
        public string? Slug { get; init; }

        public string? ConversationId { get; init; }

        public string? Prompt { get; init; }

        public string[]? SelectedAreas { get; init; }

        public string[]? SelectedCollections { get; init; }

        public PlannerPlan? Plan { get; init; }

        public List<UserEdit>? UserEdits { get; init; }
    }

    private sealed class PlannerPlan
    {
        public NamingConventions? Conventions { get; init; }

        public List<PlanEntry>? Entries { get; init; }
    }
}
