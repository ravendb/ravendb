using System.Security.Cryptography;
using Raven.AiAppliance.AiHelper;
using Raven.AiAppliance.Contracts;
using Raven.AiAppliance.Raven;
using Raven.AiAppliance.Schema;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Session;
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
        group.MapPost("/{slug}/setup/channel", ProvisionChannelAsync)
            .WithName("apps.provisionChannel")
            .Accepts<ProvisionChannelRequest>("application/json")
            .Produces<ProvisionChannelResponse>()
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
        App? app;
        using (var session = store.OpenAsyncSession())
            app = await session.LoadAsync<App>($"apps/{slug}", ct);

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
        using var session = store.OpenAsyncSession();
        var app = await session.LoadAsync<App>($"apps/{slug}", ct);

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
        App? app;
        using (var session = store.OpenAsyncSession())
        {
            // LoadAsync (not Query) because the App doc id is slug-keyed
            // (apps/{slug}, set in W6) — no index, no staleness race against
            // an immediately-prior Provision call in the wizard chain.
            app = await session.LoadAsync<App>($"apps/{slug}", ct);
        }

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

    // Channel-instance constants. M2/M4 caps prevent unbounded doc growth +
    // give the future /embed/{widgetId} page a trustworthy list to consume.
    private const int MaxAllowedOrigins = 32;
    private const int MaxOriginLength = 256;
    private const int MaxDisplayNameLength = 200;

    // Channel-type label per design §3.4 (capital I + F). Single source of
    // truth so DisplayName's default-to-Type behaviour (per the XML doc on
    // ProvisionChannelRequest) actually does default to the persisted type
    // and not a literal "iframe" string.
    private const string IFrameType = "IFrame";

    private static async Task<IResult> ProvisionChannelAsync(
        string slug,
        ProvisionChannelRequest body,
        IDocumentStore store,
        IAgentSchemaRegistry schemas,
        ILogger<AppsLogger> logger,
        CancellationToken ct)
    {
        if (body is null || string.IsNullOrWhiteSpace(body.Type) || string.IsNullOrWhiteSpace(body.AgentId))
            return Results.BadRequest(new ApiErrorResponse("type and agentId are required"));

        // 8-week demo: iFrame only. Telegram + WhatsApp are deferred per
        // design §3.6 / §3.7. Case-insensitive — the design spells it "IFrame"
        // but tests / curl will commonly send "iframe".
        if (!string.Equals(body.Type, "iframe", StringComparison.OrdinalIgnoreCase))
            return Results.BadRequest(new ApiErrorResponse($"unsupported channel type '{body.Type}'. Supported: iframe."));

        // L1: load App first so unknown-slug always returns 404 regardless of
        // whether agentId is valid. Previously the schemas.TryGet ran first,
        // and the 400-vs-404 differential leaked which agentIds were
        // registered to unauthenticated probers (M1's lack of auth made this
        // a real concern; once auth lands the leak becomes moot, but the
        // reorder is free either way).
        App? app;
        using (var session = store.OpenAsyncSession())
        {
            // LoadAsync (not Query) — see ProvisionAgentAsync comment.
            app = await session.LoadAsync<App>($"apps/{slug}", ct);
        }

        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        // L3: validate against the registry AND adopt the registry's canonical
        // casing for storage. schemas.TryGet is case-insensitive but the
        // caller's original casing was being persisted, which would trip up
        // any later case-sensitive groupBy / query on AgentId.
        if (!schemas.TryGet(body.AgentId, out var schema))
            return Results.BadRequest(new ApiErrorResponse($"unknown agentId '{body.AgentId}'"));

        // M2: validate AllowedOrigins at intake so the future /embed/{widgetId}
        // page reads from a trustworthy list. Reject "*" (silently widens trust
        // across all customers via operator typo), scheme-less entries (no
        // ambiguity about which protocol to allow), and cap entries + per-entry
        // length to keep the doc bounded under repeated edits.
        var origins = body.AllowedOrigins ?? [];
        if (origins.Length > MaxAllowedOrigins)
            return Results.BadRequest(new ApiErrorResponse($"allowedOrigins exceeds limit of {MaxAllowedOrigins} entries"));

        for (var i = 0; i < origins.Length; i++)
        {
            var origin = origins[i];
            if (string.IsNullOrWhiteSpace(origin) || origin.Length > MaxOriginLength)
                return Results.BadRequest(new ApiErrorResponse($"allowedOrigins entry is empty or exceeds {MaxOriginLength} chars"));

            if (origin == "*")
                return Results.BadRequest(new ApiErrorResponse("wildcard '*' is not an allowed origin; list explicit http(s) origins"));

            // C3 (Copilot review #4362803113): the browser Origin header is
            // scheme+host[:port] only — paths, queries, and fragments don't
            // appear at runtime. Accepting them at intake meant the persisted
            // entry would silently fail to match the browser's Origin string
            // on the future /embed/{widgetId} page. Reject anything past the
            // authority, except for a bare "/" path which Uri normalizes onto
            // origin-only URLs.
            if (!Uri.TryCreate(origin, UriKind.Absolute, out var uri) ||
                (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps) ||
                (uri.AbsolutePath != "" && uri.AbsolutePath != "/") ||
                string.IsNullOrEmpty(uri.Query) == false ||
                string.IsNullOrEmpty(uri.Fragment) == false)
                return Results.BadRequest(new ApiErrorResponse($"allowedOrigins entry '{origin}' is not an origin (scheme+host[:port] only)"));

            // C2 (Copilot review #4365219160): normalize on persist. `https://example.com/`
            // passes the path gate above (AbsolutePath is `/`) but its raw form
            // carries a trailing slash that the browser Origin header never
            // does. Persist the canonical `scheme://host[:port]` form so
            // runtime matching at /embed/{widgetId} doesn't have to defensively
            // strip slashes on every read. Uri.Authority handles default-port
            // stripping for us.
            origins[i] = $"{uri.Scheme}://{uri.Authority}";
        }

        // M4: cap DisplayName length and forbid control chars at intake. The
        // XML doc on Channel.DisplayName says "shown in the dashboard";
        // if the dashboard ever does dangerouslySetInnerHTML / equivalent,
        // unsanitized DisplayName is operator-on-operator stored XSS. Intake
        // sanitation = caught once; downstream sanitation = caught every read.
        if (body.DisplayName is not null)
        {
            if (body.DisplayName.Length > MaxDisplayNameLength)
                return Results.BadRequest(new ApiErrorResponse($"displayName exceeds {MaxDisplayNameLength} chars"));

            if (body.DisplayName.Any(char.IsControl))
                return Results.BadRequest(new ApiErrorResponse("displayName contains control characters"));
        }

        // C2 (Copilot review #4362803113): idempotency on (slug, type, agentId)
        // via an atomic guard on a deterministic binding doc id. The previous
        // query-then-put was only race-safe for sequential retries (M3's
        // WaitForNonStaleResults absorbed index staleness); concurrent POSTs
        // could both miss the query and create duplicate channel docs.
        //
        // Mechanism: write the binding doc AND the channel doc in one
        // TransactionMode.ClusterWide session — RavenDB auto-creates an
        // atomic guard at "rvn-atomic/{bindingId}" (see
        // ClusterWideTransactionHelper). Concurrent writers Raft-serialize;
        // the loser catches ClusterTransactionConcurrencyException and reads
        // the winner's binding to return the same widgetId.
        var bindingId = $"channel-bindings/{app.Slug}/{IFrameType}/{schema.Identifier}";

        // Fast path: the common "operator double-click / client retry" case
        // skips the cluster-wide round trip entirely.
        using (var session = store.OpenAsyncSession(app.Database))
        {
            var existing = await session.LoadAsync<ChannelBinding>(bindingId, ct);
            if (existing is not null)
            {
                logger.LogInformation(
                    "Channel binding already exists for slug={Slug} agentId={AgentId}; returning existing widgetId={WidgetId}",
                    app.Slug, schema.Identifier, existing.WidgetId);
                return Results.Ok(new ProvisionChannelResponse(existing.WidgetId));
            }
        }

        // Slow path: no binding yet. Try to create one atomically.
        //
        // H1 (security review 2026-05-25): widgetId is the stable customer-
        // facing identifier baked into embed snippets and the bearer
        // credential for the future anonymous /embed/{widgetId} page. 128
        // bits from RandomNumberGenerator keeps it unguessable — note that
        // we do NOT derive it from the binding tuple, because slug + type +
        // agentId are operator/public-facing inputs (Shape C from the
        // planning doc was rejected for this reason).
        var widgetId = "wgt_" + Convert.ToBase64String(RandomNumberGenerator.GetBytes(16))
            .TrimEnd('=').Replace('+', '-').Replace('/', '_');
        var channelDocId = $"@channels/{widgetId}";

        try
        {
            using var session = store.OpenAsyncSession(new global::Raven.Client.Documents.Session.SessionOptions
            {
                Database = app.Database,
                TransactionMode = TransactionMode.ClusterWide,
            });

            await session.StoreAsync(new ChannelBinding
            {
                Id = bindingId,
                WidgetId = widgetId,
                CreatedAt = DateTime.UtcNow,
            }, ct);

            await session.StoreAsync(new Channel
            {
                Id = channelDocId,
                Type = IFrameType,
                DisplayName = body.DisplayName ?? IFrameType,
                AgentId = schema.Identifier,
                AllowedOrigins = origins,
                CreatedAt = DateTime.UtcNow,
                BindingId = bindingId,
            }, ct);

            await session.SaveChangesAsync(ct);

            logger.LogInformation(
                "Provisioned channel slug={Slug} widgetId={WidgetId} agentId={AgentId} type={Type}",
                app.Slug, widgetId, schema.Identifier, IFrameType);

            return Results.Ok(new ProvisionChannelResponse(widgetId));
        }
        catch (ClusterTransactionConcurrencyException)
        {
            // Lost the race. The winner's binding is now committed (the
            // atomic guard fired BECAUSE the binding doc id we tried to
            // create already exists). Read it back and return the same
            // widgetId — the concurrent caller sees idempotent success.
            using var session = store.OpenAsyncSession(app.Database);
            var winner = await session.LoadAsync<ChannelBinding>(bindingId, ct);
            if (winner is null)
            {
                throw new InvalidOperationException(
                    $"ClusterTransactionConcurrencyException fired for '{bindingId}' but the binding doc could not be loaded after the conflict.");
            }

            logger.LogInformation(
                "Lost race for binding slug={Slug} agentId={AgentId}; returning winner's widgetId={WidgetId}",
                app.Slug, schema.Identifier, winner.WidgetId);
            return Results.Ok(new ProvisionChannelResponse(winner.WidgetId));
        }
    }

    /// Logger category marker — keeps the ILogger generic-arg out of the public surface.
    internal sealed class AppsLogger;
}
