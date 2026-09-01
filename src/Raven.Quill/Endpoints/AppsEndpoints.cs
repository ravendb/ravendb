using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Quill.Agents;
using Raven.Quill.AiHelper;
using Raven.Quill.Cdc;
using Raven.Quill.Contracts;
using Raven.Quill.Discord;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Logging;
using Raven.Quill.Live;
using Raven.Quill.Raven;
using Raven.Quill.Slack;
using Raven.Quill.Telegram;
using Raven.Quill.Wizard;
using Raven.Server.Logging;

namespace Raven.Quill.Endpoints;

public static class AppsEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps").WithTags("apps").RequireAuthorization();
        group.MapGet("/", ListAsync)
            .WithName("apps.list")
            .Produces<AppResponse[]>();
        group.MapGet("/{slug}", GetAsync)
            .WithName("apps.detail")
            .Produces<AppResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
        group.MapPost("/{slug}/setup/agent", ProvisionAgentAsync)
            .WithName("apps.provisionAgent")
            .Accepts<EditAgentRequest>("application/json")
            .Produces<ProvisionAgentResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
        group.MapGet("/{slug}/cdc/progress", CdcProgressAsync)
            .WithName("apps.cdcProgress")
            // WS-only route; OpenAPI can't describe the upgrade + streamed frames
            .ExcludeFromDescription();
        group.MapGet("/{slug}/cdc/performance", CdcPerformanceAsync)
            .WithName("apps.cdcPerformance")
            .Produces<CdcPerformanceResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
        group.MapGet("/{slug}/cdc/errors", CdcErrorsAsync)
            .WithName("apps.cdcErrors")
            .Produces<CdcError[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
        group.MapGet("/{slug}/cdc", GetCdcAsync)
            .WithName("apps.cdcGet")
            .Produces<AppCdcConfigurationResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
        group.MapPost("/{slug}/cdc/restart", RestartCdcAsync)
            .WithName("apps.cdcRestart")
            .WithDescription(
                "Restarts the app's CDC task by disabling and re-enabling it. A task that is already " +
                "disabled is left alone; enable it instead.")
            .Produces(StatusCodes.Status204NoContent)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound)
            .Produces<ApiErrorResponse>(StatusCodes.Status409Conflict)
            .Produces<ApiErrorResponse>(StatusCodes.Status500InternalServerError);

        group.MapPost("/{slug}/setup/try", SetupTryAsync)
            .WithName("apps.setupTry")
            .Accepts<SetupTryRequest>("application/json")
            // streams NDJSON frames, not a single body
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

        group.MapDelete("/{slug}", DeleteAppAsync)
            .WithName("apps.delete")
            .Produces(StatusCodes.Status204NoContent)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/{slug}/connection-strings", ListConnectionStringsAsync)
            .WithName("apps.aiConnectionStringsList")
            .Produces<List<AiConnectionString>>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
    }

    private static async Task<IResult> ListConnectionStringsAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);

        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var r = await store.Maintenance.ForDatabase(slug).SendAsync(new GetConnectionStringsOperation(), ct);
        return Results.Ok(r.AiConnectionStrings?.Values.ToList() ?? []);
    }

    private static async Task<IResult> DeleteAppAsync(
        string slug,
        IDocumentStore store,
        ITelegramChannelManager telegramManager,
        SlackHealthRegistry slackHealth,
        IDiscordChannelManager discordManager,
        DiscordHealthRegistry discordHealth,
        QuillLogger<AppsLogger> logger,
        HttpContext ctx,
        CancellationToken ct)
    {
        if (logger.IsInfoEnabled) 
            logger.Info($"Deleting app with slug={slug}");

        var app = await AppLookup.LoadAppAsync(store, slug, ct);

        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(slug, true), ct);
        await AppLookup.DeleteAppAsync(store, slug, ct);

        slackHealth.RemoveDatabase(app.Database);
        discordHealth.RemoveDatabase(app.Database);

        if (logger.AuditEnabled)
            logger.Audit("DELETE", $"App '{slug}' (database={slug})", ctx);

        telegramManager.Wake();
        discordManager.Wake();

        return Results.NoContent();
    }

    private static async Task<IResult> SuggestAgentAsync(
        string slug,
        SuggestAgentRequest body,
        IDocumentStore store,
        IAiHelperClient aiClient,
        QuillLogger<AppsLogger> logger,
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

        var cdcConfig = (await AppLookup.LoadCdcTaskAsync(store, app, ct))?.Configuration;

        if (mode == "from-data" && cdcConfig is null)
            return Results.BadRequest(new ApiErrorResponse(
                $"app '{slug}' has no CDC configuration to derive an agent from; use mode 'from-prompt'"));

        var result = await aiClient.SuggestAiAgentAsync(
            cdcConfig ?? new CdcSinkConfiguration(), collectionsSample: null, mode, body!.IntentPrompt, ct);

        if (result.Status != AiHelperStatus.Success)
            return Results.Ok(new SuggestAgentResponse([], result.Rationale, result.Status.ToString()));

        var maxCandidates = mode == "from-prompt" ? 1 : 3;
        var valid = result.Configurations.Where(IsStructurallyValidDraft).Take(maxCandidates).ToArray();
        if (valid.Length == 0)
        {
            if (logger.IsInfoEnabled)
                logger.Info($"SuggestAgent: no structurally valid candidate returned for slug={slug}");
            return Results.UnprocessableEntity(new ApiErrorResponse("AI service returned no structurally valid agent configuration"));
        }

        return Results.Ok(new SuggestAgentResponse(valid, result.Rationale, result.Status.ToString()));
    }

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
        EditAgentRequest request,
        IDocumentStore store,
        QuillLogger<AppsLogger> logger,
        HttpContext ctx,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);

        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var validationError = await AgentConfigValidator.ValidateAndPrepareAsync(store, slug, request, ct);
        if (validationError is not null)
            return validationError;

        var body = request.Configuration;
        
        if (logger.IsInfoEnabled)
            logger.Info(
                $"Provisioning agent for app slug={app.Slug} name={body.Name} " +
                $"identifier={(string.IsNullOrWhiteSpace(body.Identifier) ? "(server-assigned)" : body.Identifier)} " +
                $"cs={body.ConnectionStringName} queries={body.Queries?.Count ?? 0}, " +
                $"webhooks={request.ActionBindings?.Count ?? 0}");

        try
        {
            var result = await AiAgentRegistrar.RegisterAsync(store, body, app.Database, ct);
            await AiAgentRegistrar.RegisterBindingsAsync(store, app.Database, result.Identifier, request.ActionBindings, ct);
            if (logger.AuditEnabled)
                logger.Audit("POST",
                    $"AiAgentConfiguration '{result.Identifier}' in App '{app.Slug}' " +
                    $"actions=[{AgentActionBindings.DescribeTargetsForAudit(request.ActionBindings)}]",
                    ctx);
            return Results.Ok(new ProvisionAgentResponse(result.Identifier));
        }
        // map RavenDB validation to a 400 instead of a leaked 500
        catch (RavenException ex)
        {
            if (logger.IsWarnEnabled)
                logger.Warn(ex,
                    $"Agent provisioning rejected by RavenDB for app slug={app.Slug} name={body.Name}");
            return Results.BadRequest(new ApiErrorResponse("agent configuration rejected; see server logs for details"));
        }
    }

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
        await RavenLiveFeedProxy.RelayAsync(browser, store, app.Database, $"cdc-sink/performance/live?name={app.CdcTaskName}", ct);
    }

    private static async Task<IResult> CdcPerformanceAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var cdc = await AppLookup.LoadCdcTaskAsync(store, app, ct);
        if (cdc is null)
            return Results.NotFound(AppLookup.NoCdcTaskError(slug));

        var raw = await CdcPerformanceReader.ReadAsync(store.Maintenance.ForDatabase(app.Database), ct);
        var (state, lastModified) = await AppLookup.LoadCdcStateAsync(store, app.Database, app.CdcTaskName, ct);

        var snapshot = CdcPerformanceShaper.Shape(
            raw, disabled: cdc.Configuration.Disabled, DateTime.UtcNow, lastActivityAt: lastModified);
        return Results.Ok(snapshot);
    }

    private static async Task<IResult> CdcErrorsAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var raw = await CdcPerformanceReader.ReadErrorsAsync(store.Maintenance.ForDatabase(app.Database), ct);
        return Results.Ok(CdcPerformanceShaper.ShapeErrors(raw));
    }

    private static async Task<IResult> GetCdcAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var cdc = await AppLookup.LoadCdcTaskAsync(store, app, ct);
        if (cdc is null)
            return Results.NotFound(AppLookup.NoCdcTaskError(slug));

        return Results.Ok(new AppCdcConfigurationResponse(
            cdc.Configuration,
            await LoadSourceConnectionStringAsync(store, app.Database, cdc.Configuration.ConnectionStringName, ct)));
    }

    private static async Task<IResult> RestartCdcAsync(
        string slug,
        IDocumentStore store,
        ILogger<AppsLogger> logger,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var cdc = await AppLookup.LoadCdcTaskAsync(store, app, ct);
        if (cdc is null)
            return Results.NotFound(AppLookup.NoCdcTaskError(slug));

        if (cdc.Configuration.Disabled)
            return Results.Conflict(new ApiErrorResponse(
                $"cdc task for '{slug}' is disabled; enable it instead of restarting"));

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation(
                "Restarting CDC task {TaskName} (id={TaskId}) for app slug={Slug}", app.CdcTaskName, cdc.TaskId, slug);

        var maintenance = store.Maintenance.ForDatabase(app.Database);
        await maintenance.SendAsync(
            new ToggleOngoingTaskStateOperation(cdc.TaskId, OngoingTaskType.CdcSink, disable: true), ct);

        // Past this point the sink is stopped, so the re-enable must not ride on the request token:
        // a caller that navigates away would otherwise leave the sync disabled with nobody told.
        try
        {
            await maintenance.SendAsync(
                new ToggleOngoingTaskStateOperation(cdc.TaskId, OngoingTaskType.CdcSink, disable: false),
                CancellationToken.None);
        }
        catch (Exception e)
        {
            logger.LogError(e,
                "Failed to re-enable CDC task {TaskName} (id={TaskId}) for app slug={Slug}; the sync is left disabled",
                app.CdcTaskName, cdc.TaskId, slug);
            return Results.Json(
                new ApiErrorResponse($"the sync for '{slug}' was stopped but could not be restarted; it is now disabled"),
                statusCode: StatusCodes.Status500InternalServerError);
        }

        return Results.NoContent();
    }

    private static async Task<string?> LoadSourceConnectionStringAsync(
        IDocumentStore store,
        string database,
        string? connectionStringName,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(connectionStringName))
            return null;

        var result = await store.Maintenance.ForDatabase(database).SendAsync(
            new GetConnectionStringsOperation(connectionStringName, ConnectionStringType.Sql), ct);
        return result.SqlConnectionStrings.TryGetValue(connectionStringName, out var source)
            ? source.ConnectionString
            : null;
    }

    private static async Task SetupTryAsync(
        string slug,
        SetupTryRequest body,
        IDocumentStore store,
        QuillLogger<AppsLogger> logger,
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

        AiAgentRegistrar.EnsureDefaultOutputShape(body.Configuration);

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
                async chunk => await NdjsonStream.WriteLineAsync(ctx, new { type = "chunk", text = chunk }),
                ct);

            var result = await store.Maintenance.ForDatabase(app.Database).SendAsync(operation, ct);

            await NdjsonStream.WriteLineAsync(ctx, new
            {
                type = "done",
                answer = new { reply = result.Reply },
                fullAnswer = result.Answer,
                toolCalls = result.ToolCalls,
                conversationId = result.ConversationId,
            });
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception e)
        {
            if (logger.IsErrorEnabled)
                logger.Error(e, $"setup/try failed for slug={slug}");
            try
            {
                await NdjsonStream.WriteLineAsync(ctx, new { type = "error", message = "Agent test failed. See server logs for details." });
            }
            catch
            {
            }
        }
    }

    internal sealed class AppsLogger;
}
