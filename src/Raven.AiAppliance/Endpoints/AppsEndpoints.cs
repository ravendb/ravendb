using Raven.AiAppliance.Agents;
using Raven.AiAppliance.AiHelper;
using Raven.AiAppliance.Contracts;
using Raven.AiAppliance.Endpoints.Helpers;
using Raven.AiAppliance.Live;
using Raven.AiAppliance.Raven;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;

namespace Raven.AiAppliance.Endpoints;

public static class AppsEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps").WithTags("apps");
        group.MapGet("/", ListAsync)
            .WithName("apps.list")
            .Produces<AppResponse[]>();
        group.MapGet("/{slug}", GetAsync)
            .WithName("apps.detail")
            .Produces<AppResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
        group.MapPost("/{slug}/setup/agent", ProvisionAgentAsync)
            .WithName("apps.provisionAgent")
            .Accepts<AiAgentConfiguration>("application/json")
            .Produces<ProvisionAgentResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
        group.MapGet("/{slug}/cdc/progress", CdcProgressAsync)
            .WithName("apps.cdcProgress")
            // WebSocket-only route (101 on success); OpenAPI can't describe the
            // upgrade + streamed frames, so keep it out of the spec like /embed/*.
            .ExcludeFromDescription();
        group.MapPost("/{slug}/setup/try", SetupTryAsync)
            .WithName("apps.setupTry")
            .Accepts<SetupTryRequest>("application/json")
            // Streams NDJSON frames (chunk/done/error), not a single body — declare the status +
            // content type only, without a (misleading) string body schema.
            .Produces(StatusCodes.Status200OK, contentType: "application/x-ndjson")
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
        group.MapPost("/{slug}/suggest/agent", SuggestAgentAsync)
            .WithName("apps.suggestAgent")
            .Accepts<SuggestAgentRequest>("application/json")
            .Produces<SuggestAgentResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound)
            .Produces<ApiErrorResponse>(StatusCodes.Status422UnprocessableEntity);
    }

    /// <summary>
    /// AI-suggest an agent for a provisioned app. Reads the app's current
    /// <see cref="CdcSinkConfiguration"/> from the per-app DB record, asks the internal AI service
    /// for draft <see cref="AiAgentConfiguration"/> candidate(s) (1-3 for <c>from-data</c>, one for
    /// <c>from-prompt</c>), structurally re-validates them, and returns them for the editable agent
    /// Review form. <b>Generate-only</b>: provisioning stays on the existing <c>/setup/agent</c> flow.
    /// <para>
    /// <c>collectionsSample</c> is omitted in this milestone: sending a capped sample of mirrored docs
    /// for richer LLM context is a follow-up; the internal contract marks samples optional.
    /// </para>
    /// </summary>
    private static async Task<IResult> SuggestAgentAsync(
        string slug,
        SuggestAgentRequest body,
        IDocumentStore store,
        IAiHelperClient aiClient,
        ILogger<AppsLogger> logger,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);

        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var mode = body?.Mode?.Trim().ToLowerInvariant();
        if (mode != "from-data" && mode != "from-prompt")
            return Results.BadRequest(new ApiErrorResponse("mode must be 'from-data' or 'from-prompt'"));

        if (mode == "from-prompt" && string.IsNullOrWhiteSpace(body!.IntentPrompt))
            return Results.BadRequest(new ApiErrorResponse("intentPrompt is required for from-prompt mode"));

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(app.Database), ct);
        var cdcConfig = record?.CdcSinks?.FirstOrDefault();

        if (mode == "from-data" && cdcConfig is null)
            return Results.BadRequest(new ApiErrorResponse(
                $"app '{slug}' has no CDC configuration to derive an agent from; use mode 'from-prompt'"));

        var result = await aiClient.SuggestAiAgentAsync(
            cdcConfig ?? new CdcSinkConfiguration(), collectionsSample: null, mode, body!.IntentPrompt, ct);

        if (result.Status != AiHelperStatus.Success)
            return Results.Ok(new SuggestAgentResponse([], result.Rationale, result.Status.ToString()));

        // Enforce the API contract: from-prompt yields one candidate, from-data up to three.
        // Cap an over-eager or malformed upstream response at that limit.
        var maxCandidates = mode == "from-prompt" ? 1 : 3;
        var valid = result.Configurations.Where(IsStructurallyValidDraft).Take(maxCandidates).ToArray();
        if (valid.Length == 0)
        {
            logger.LogInformation("SuggestAgent: no structurally valid candidate returned for slug={Slug}", slug);
            return Results.UnprocessableEntity(new ApiErrorResponse("AI service returned no structurally valid agent configuration"));
        }

        return Results.Ok(new SuggestAgentResponse(valid, result.Rationale, result.Status.ToString()));
    }

    /// A draft agent is editable in the Review form, so we only require the fields the LLM must
    /// author: Name and SystemPrompt. Identifier is not required: provisioning
    /// server-assigns one when empty (see ProvisionAgentAsync), so requiring it here would discard
    /// otherwise-usable candidates. ConnectionStringName is likewise chosen by the operator at
    /// provision time.
    private static bool IsStructurallyValidDraft(AiAgentConfiguration agent) =>
        !string.IsNullOrWhiteSpace(agent.Name) &&
        !string.IsNullOrWhiteSpace(agent.SystemPrompt);

    private static async Task<IResult> ListAsync(
        IDocumentStore store,
        CancellationToken ct)
    {
        using var session = store.OpenAsyncSession();
        var apps = await session.Query<App>()
            .OrderByDescending(x => x.CreatedAt)
            .ToListAsync(ct);

        return Results.Ok(apps.Select(AppResponse.From).ToArray());
    }

    private static async Task<IResult> GetAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);

        return app is null
            ? Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"))
            : Results.Ok(AppResponse.From(app));
    }

    private static async Task<IResult> ProvisionAgentAsync(
        string slug,
        AiAgentConfiguration body,
        IDocumentStore store,
        ILogger<AppsLogger> logger,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);

        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        // The operator-defined agent arrives as a RavenDB AiAgentConfiguration
        // (same pattern as the AI connection-strings endpoint). STJ binds via
        // the parameterless ctor, which bypasses the 3-arg ctor's guards, so this
        // validates the required fields here and returns a 400 instead of a 500
        // from deep in the client.
        if (body is null)
            return Results.BadRequest(new ApiErrorResponse("request body is required"));

        if (string.IsNullOrWhiteSpace(body.Name))
            return Results.BadRequest(new ApiErrorResponse("name is required"));

        if (string.IsNullOrWhiteSpace(body.SystemPrompt))
            return Results.BadRequest(new ApiErrorResponse("systemPrompt is required"));

        if (string.IsNullOrWhiteSpace(body.ConnectionStringName))
            return Results.BadRequest(new ApiErrorResponse("connectionStringName is required"));

        // Demo subset: model-side Actions and server-side SubAgents aren't
        // smoke-tested in the 8-week scope (and AiAgentToolSubAgent uses public
        // fields that won't round-trip under default STJ anyway). Reject at
        // intake rather than silently provisioning an agent that misbehaves.
        if (body.Actions is { Count: > 0 })
            return Results.BadRequest(new ApiErrorResponse("actions are not supported in demo"));

        if (body.SubAgents is { Count: > 0 })
            return Results.BadRequest(new ApiErrorResponse("subAgents are not supported in demo"));

        // Look up the operator's AI connection string on the per-app DB. The
        // CS was POSTed via /api/apps/{slug}/ai/connection-strings (the
        // dashboard's "pick existing OR add new" step). Defence in depth:
        // re-gate ModelType + provider here, since a CS that landed via direct
        // RavenDB Studio would bypass the POST-time gate.
        var cs = await store.Maintenance.ForDatabase(app.Database)
            .SendAsync(new GetConnectionStringsOperation(body.ConnectionStringName, ConnectionStringType.Ai), ct);

        if (cs.AiConnectionStrings is null ||
            cs.AiConnectionStrings.TryGetValue(body.ConnectionStringName, out var aiCs) == false)
        {
            return Results.BadRequest(new ApiErrorResponse(
                $"connection string '{body.ConnectionStringName}' not found; create it via " +
                $"POST /api/apps/{slug}/ai/connection-strings first"));
        }

        if (aiCs.ModelType != AiModelType.Chat)
            return Results.BadRequest(new ApiErrorResponse(
                $"connection string '{aiCs.Name}' has ModelType={aiCs.ModelType}; agent provisioning requires Chat"));

        var provider = aiCs.GetActiveProvider();
        if (provider != AiConnectorType.OpenAi && provider != AiConnectorType.Ollama)
            return Results.BadRequest(new ApiErrorResponse(
                $"connection string '{aiCs.Name}' uses unsupported provider '{provider}' in demo; supported: OpenAi, Ollama"));

        // Over-posting control: the operator can't ship a disabled agent through
        // the wizard. Identifier stays operator-supplied (or server-assigned
        // when empty); Queries/Parameters/ChatTrimming are taken as-is.
        body.Disabled = false;

        logger.LogInformation(
            "Provisioning agent for app slug={Slug} name={Name} identifier={Identifier} cs={ConnectionStringName} queries={QueryCount}",
            app.Slug, body.Name, string.IsNullOrWhiteSpace(body.Identifier) ? "(server-assigned)" : body.Identifier,
            aiCs.Name, body.Queries?.Count ?? 0);

        // RegisterAsync proxies to RavenDB's AddOrUpdateAiAgentOperation, whose
        // server-side ValidateConfiguration rejects operator input the intake gates
        // above don't replicate (invalid tool-query names, malformed ChatTrimming,
        // duplicate parameters, unparseable RQL). Surface those as a 400 instead of
        // letting the RavenException bubble out as a 500. This matches the "don't leak
        // RavenDB's 500" stance the AI connection-strings endpoint takes at intake.
        try
        {
            var result = await AiAgentRegistrar.RegisterAsync(store, body, app.Database, ct);
            return Results.Ok(new ProvisionAgentResponse(result.Identifier));
        }
        catch (RavenException ex)
        {
            logger.LogWarning(ex,
                "Agent provisioning rejected by RavenDB for app slug={Slug} name={Name}", app.Slug, body.Name);
            return Results.BadRequest(new ApiErrorResponse($"agent configuration rejected: {ex.Message}"));
        }
    }

    /// <summary>
    /// Live CDC initial-load progress over a WebSocket. Wizard read-side
    /// (design §1.3 Stage C.1 step 7 / RavenDB-26629 carryover). The bridge
    /// proxies RavenDB's native <c>cdc-sink/performance/live</c> feed (the same
    /// telemetry Studio renders) via <see cref="RavenLiveFeedProxy"/> — the
    /// browser can't present a client cert, so the bridge dials RavenDB with the
    /// admin cert and relays frames. App is resolved (404) and the WS upgrade is
    /// required (400) before the handshake is accepted.
    /// </summary>
    private static async Task CdcProgressAsync(
        string slug,
        IDocumentStore store,
        HttpContext ctx)
    {
        var ct = ctx.RequestAborted;

        var app = await AppLookup.LoadAppAsync(store, slug, ct);

        if (app is null)
        {
            ctx.Response.StatusCode = StatusCodes.Status404NotFound;
            await ctx.Response.WriteAsJsonAsync(new ApiErrorResponse($"no app with slug '{slug}'"), ct);
            return;
        }

        if (ctx.WebSockets.IsWebSocketRequest == false)
        {
            ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
            await ctx.Response.WriteAsJsonAsync(new ApiErrorResponse("websocket upgrade required"), ct);
            return;
        }

        using var browser = await ctx.WebSockets.AcceptWebSocketAsync();
        await RavenLiveFeedProxy.RelayAsync(browser, store, app.Database, "cdc-sink/performance/live", ct);
    }

    /// <summary>
    /// "Test agent" smoke test (design §1.3 Stage C.2 step 12 / RavenDB-26629
    /// carryover). Streams a single turn against the <em>draft</em> configuration the
    /// operator is editing in the wizard's Review step — before it is provisioned — via
    /// <see cref="RunDraftAgentTestOperation"/> (RavenDB's agent test endpoint), relaying
    /// the reply chunks as NDJSON so the operator can confirm it answers before saving.
    /// The conversation is not persisted, so each turn is independent.
    /// </summary>
    private static async Task SetupTryAsync(
        string slug,
        SetupTryRequest body,
        IDocumentStore store,
        ILogger<AppsLogger> logger,
        HttpContext ctx)
    {
        var ct = ctx.RequestAborted;

        if (body is null || string.IsNullOrWhiteSpace(body.Prompt) || body.Configuration is null)
        {
            ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
            await ctx.Response.WriteAsJsonAsync(new ApiErrorResponse("prompt and configuration are required"), ct);
            return;
        }

        var app = await AppLookup.LoadAppAsync(store, slug, ct);

        if (app is null)
        {
            ctx.Response.StatusCode = StatusCodes.Status404NotFound;
            await ctx.Response.WriteAsJsonAsync(new ApiErrorResponse($"no app with slug '{slug}'"), ct);
            return;
        }

        // Give the smallest draft (name + prompt + connection string) the same default output
        // shape provisioning applies, so RavenDB's agent validation accepts it.
        AiAgentRegistrar.EnsureDefaultOutputShape(body.Configuration);

        // The operator can pick which output field streams token-by-token (the wizard's
        // "Streamed field" select); fall back to the conventional first-declared field when unset.
        var streamField = string.IsNullOrWhiteSpace(body.StreamField)
            ? AgentOutputShape.ResolveReplyField(body.Configuration)
            : body.StreamField.Trim();

        NdjsonStream.SetHeaders(ctx);
        try
        {
            var operation = new RunDraftAgentTestOperation(
                body.Configuration,
                body.Prompt,
                body.Parameters,
                streamField,
                async chunk => await NdjsonStream.WriteLineAsync(ctx, new { type = "chunk", text = chunk }));

            var result = await store.Maintenance.ForDatabase(app.Database).SendAsync(operation, ct);

            await NdjsonStream.WriteLineAsync(ctx, new
            {
                type = "done",
                // Normalized single reply (casing-safe) — the no-stream fallback for the chat bubble.
                answer = new { reply = result.Reply },
                // Full structured model output, so the wizard can render the whole JSON answer
                // (not just the streamed field). Omitted when the turn produced no object.
                fullAnswer = result.Answer,
                // The query tools the agent ran this turn (RQL, model-filled parameters, and the
                // returned content), so the wizard can show the transcript. Empty when none ran.
                toolCalls = result.ToolCalls,
                conversationId = result.ConversationId,
            });
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Client disconnected mid-stream.
        }
        catch (Exception e)
        {
            // Both an invalid draft (RavenDB validation) and a model/provider failure surface
            // here. Log full detail server-side; emit a generic error frame — raw exceptions can
            // leak DB names / connection strings / file paths.
            logger.LogError(e, "setup/try failed for slug={Slug}", slug);
            try
            {
                await NdjsonStream.WriteLineAsync(ctx, new { type = "error", message = "Agent test failed. See server logs for details." });
            }
            catch
            {
                // Response may already be partially flushed.
            }
        }
    }

    /// Logger category marker — keeps the ILogger generic-arg out of the public surface.
    internal sealed class AppsLogger;
}
