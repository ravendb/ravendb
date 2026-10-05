using System.IO.Compression;
using System.Net;
using System.Text;
using System.Text.Json;
using Raven.Client.Documents;
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
    public sealed record Refusal(string Message, AiHelperStatus? Status = null);

    public async Task<Refusal?> StartAsync(
        MigrationStartRequest request,
        Func<MigrationFrame, Task> onFrame,
        CancellationToken token)
    {
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
        if (string.IsNullOrWhiteSpace(request.Prompt))
            return new Refusal("prompt is required");

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
                Plan = new PlannerPlan { Conventions = plan.Conventions, Entries = plan.CurrentEntries() },
                RemovedByUser = plan.PendingUserRemovals.ToList()
            },
            request.Slug,
            request.ConversationId,
            plan,
            onFrame,
            token);

        return null;
    }

    public async Task<Refusal?> RemoveCollectionAsync(MigrationRemoveCollectionRequest request, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(request.ConversationId))
            return new Refusal("conversationId is required");
        if (string.IsNullOrWhiteSpace(request.Collection))
            return new Refusal("collection is required");

        WizardState? state;
        using (var session = store.OpenAsyncSession())
            state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(request.Slug), token);

        var plan = state is null ? null : CurrentPlan(state, request.ConversationId);
        if (plan is null)
            return new Refusal("no plan found for that conversation");

        if (plan.Contains(request.Collection) == false)
            return new Refusal($"the plan has no collection named {request.Collection}");

        plan.RemoveByUser(request.Collection);
        await plan.SaveAsync(store, request.Slug, token);

        return null;
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

        var unmapped = PlanToCdcConfiguration.UnmappedTables(configuration, coverageSchema);

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
                if (logger.IsInfoEnabled)
                    logger.Info($"Planner {path} failed: upstream {(int)response.StatusCode}.");

                await onFrame(new ErrorFrame { Message = DescribeFailure(response.StatusCode) });
                return;
            }

            await using var body = await response.Content.ReadAsStreamAsync(token);
            using var reader = new StreamReader(body, Encoding.UTF8);

            try
            {
                while (await reader.ReadLineAsync(token) is { } line)
                {
                    if (line.StartsWith(DataPrefix, StringComparison.Ordinal) == false)
                        continue;

                    var frame = ReadFrame(path, line[DataPrefix.Length..]);
                    if (frame is null)
                        continue;

                    if (frame is DoneFrame done)
                    {
                        conversationId = done.ConversationId;
                        plan.PendingUserRemovals.Clear();
                    }

                    if (Mirror(plan, frame) && conversationId is not null)
                    {
                        plan.ConversationId = conversationId;
                        await plan.SaveAsync(store, slug, token);
                    }

                    await onFrame(frame);
                }
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
                return true;

            case RemovedFrame { Collection: not null } removed:
                plan.Remove(removed.Collection);
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

    private static string DescribeFailure(HttpStatusCode statusCode) => statusCode switch
    {
        HttpStatusCode.Unauthorized => "The AI service refused the request: consent is required or the license was not accepted.",
        HttpStatusCode.TooManyRequests => "The monthly AI token quota is used up.",
        HttpStatusCode.NotFound => "No planning session found for that conversation.",
        HttpStatusCode.BadRequest => "The AI service rejected the planning request.",
        HttpStatusCode.RequestEntityTooLarge => "The selected schema is too large for the AI service. Select fewer tables.",
        _ => $"The AI service failed (HTTP {(int)statusCode})."
    };

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

    private sealed class PlannerAskRequest
    {
        public string? Slug { get; init; }

        public string? ConversationId { get; init; }

        public string? Prompt { get; init; }

        public PlannerPlan? Plan { get; init; }

        public List<string>? RemovedByUser { get; init; }
    }

    private sealed class PlannerPlan
    {
        public NamingConventions? Conventions { get; init; }

        public List<PlanEntry>? Entries { get; init; }
    }
}
