using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Metrics;
using Raven.Quill.Raven;

namespace Raven.Quill.Endpoints;

public static class AgentsEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps/{slug}").WithTags("agents").RequireAuthorization();

        group.MapGet("/agents", ListAgentsAsync)
            .WithName("agents.list")
            .Produces<AgentSummaryResponse[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/agent/{agentId}", GetAgentAsync)
            .WithName("agents.get")
            .Produces<AgentDetailsResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapDelete("/agent/{agentId}", DeleteAgentAsync)
            .WithName("agents.delete")
            .Produces(StatusCodes.Status204NoContent)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound)
            .Produces<ApiErrorResponse>(StatusCodes.Status409Conflict);

        group.MapPost("/agent", EditAgentAsync)
            .WithName("agents.edit")
            .Accepts<EditAgentRequest>("application/json")
            .Produces<ProvisionAgentResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound)
            .Produces<ApiErrorResponse>(StatusCodes.Status500InternalServerError);
    }

    private static async Task<IResult> ListAgentsAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var maintenance = store.Maintenance.ForDatabase(app.Database);

        var agents = await maintenance.SendAsync(new GetAiAgentsOperation(), ct);

        var modelByConnectionString = await MetricsReadService.ModelByConnectionStringAsync(store, slug, ct);
        var activity = await MetricsReadService.GetAgentActivityAsync(store, app.Database, ct);

        var items = (agents.AiAgents ?? [])
            .Select(agent =>
            {
                var act = activity.GetValueOrDefault(agent.Identifier);
                return new AgentSummaryResponse(
                    agent.Identifier,
                    string.IsNullOrWhiteSpace(agent.Name) ? agent.Identifier : agent.Name,
                    agent.ConnectionStringName is { } name && modelByConnectionString.TryGetValue(name, out var model)
                        ? model
                        : null,
                    agent.Disabled,
                    (agent.Parameters ?? [])
                    .Select(parameter => parameter.Name)
                    .Where(parameterName => string.IsNullOrWhiteSpace(parameterName) == false)
                    .ToArray(),
                    act?.LastInvokedAt,
                    act?.Conversations ?? 0,
                    act?.Messages ?? 0,
                    act?.Tokens ?? 0);
            })
            .OrderBy(agent => agent.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return Results.Ok(items);
    }

    // full agent config (unlike the projected list) so the UI can populate an edit form and POST it
    // back unchanged. That includes each binding's webhook secret verbatim: the operator edit form has
    // no other way to preserve it, and the reader is the same authenticated operator who set it.
    private static async Task<IResult> GetAgentAsync(
        string slug,
        string agentId,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var agent = await AgentLookup.FindAsync(store, app.Database, agentId, ct);
        if (agent is null)
            return Results.NotFound(new ApiErrorResponse($"no agent '{agentId}' in app '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var bindings = await session.LoadAsync<AgentActionBindings>(
            AgentActionBindings.IdFor(agent.Identifier), ct);

        return Results.Ok(new AgentDetailsResponse(agent, bindings?.Bindings ?? []));
    }

    private static async Task<IResult> EditAgentAsync(
        string slug,
        EditAgentRequest request,
        IDocumentStore store,
        ILogger<AgentsLogger> logger,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        if (request?.Configuration is not { } body || string.IsNullOrWhiteSpace(body.Identifier))
            return Results.BadRequest(new ApiErrorResponse("identifier is required to edit an agent"));

        // edit = update-only: the agent must already exist
        var existing = await AgentLookup.FindAsync(store, app.Database, body.Identifier, ct);
        if (existing is null)
            return Results.NotFound(new ApiErrorResponse($"no agent '{body.Identifier}' in app '{slug}'"));

        var validationError = await AgentConfigValidator.ValidateAndPrepareAsync(store, slug, request, ct);
        if (validationError is not null)
            return validationError;

        // the server binds the identifier to the name (identifier is derived from it), so a
        // same-identifier update with a different name is rejected — surface that as a clear 400
        if (string.Equals(existing.Name, body.Name, StringComparison.Ordinal) == false)
            return Results.BadRequest(new ApiErrorResponse(
                "renaming an agent is not supported; its identifier is derived from the name"));

        // canonical identifier (FindAsync is case-insensitive) so AddOrUpdate targets the same doc
        body.Identifier = existing.Identifier;

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation(
                "Editing agent slug={Slug} agentId={AgentId} name={Name}", app.Slug, body.Identifier, body.Name);

        try
        {
            var result = await AiAgentRegistrar.RegisterAsync(store, body, app.Database, ct);
            await AiAgentRegistrar.RegisterBindingsAsync(store, app.Database, result.Identifier, request.ActionBindings, ct);
            return Results.Ok(new ProvisionAgentResponse(result.Identifier));
        }
        // map RavenDB validation to a 400 instead of a leaked 500
        catch (RavenException ex)
        {
            if (logger.IsEnabled(LogLevel.Warning))
                logger.LogWarning(ex,
                    "Agent edit rejected by RavenDB for app slug={Slug} agentId={AgentId}", app.Slug, body.Identifier);
            return Results.BadRequest(new ApiErrorResponse("agent configuration rejected; see server logs for details"));
        }
    }

    private static async Task<IResult> DeleteAgentAsync(
        string slug,
        string agentId,
        IDocumentStore store,
        ILogger<AgentsLogger> logger,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var agent = await AgentLookup.FindAsync(store, app.Database, agentId, ct);
        if (agent is null)
            return Results.NotFound(new ApiErrorResponse($"no agent '{agentId}' in app '{slug}'"));

        // refuse while channels / live embed-links still point at the agent: deleting it
        // would leave them resolving to a missing agent (broken embed page)
        var bound = await CountBoundReferencesAsync(store, app.Database, agent.Identifier, ct);
        if (bound > 0)
            return Results.Conflict(new ApiErrorResponse(
                $"agent '{agentId}' still has {bound} channel(s) bound to it; remove them first"));

        await store.AI.ForDatabase(app.Database).DeleteAgentAsync(agent.Identifier, ct);

        using (var session = store.OpenAsyncSession(app.Database))
        {
            session.Delete(AgentActionBindings.IdFor(agent.Identifier));
            await session.SaveChangesAsync(ct);
        }

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Deleted agent slug={Slug} agentId={AgentId}", app.Slug, agent.Identifier);
        return Results.NoContent();
    }

    private static async Task<int> CountBoundReferencesAsync(
        IDocumentStore store, string database, string agentId, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(database);

        var channels = await session.LoadAllStartingWithAsync<Channel>(Channel.IdPrefix, ct);
        return channels.Count(c => string.Equals(c.AgentId, agentId, StringComparison.OrdinalIgnoreCase));
    }

    internal sealed class AgentsLogger;
}
