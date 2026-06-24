using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Raven.AiAppliance.Contracts;
using Raven.AiAppliance.Endpoints.Helpers;
using Raven.AiAppliance.Metrics;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;

namespace Raven.AiAppliance.Endpoints;

/// <summary>
/// Read-side listing of an app's provisioned RavenDB AI agents for the
/// dashboard overview (mirrors <see cref="AiConnectionStringsEndpoints"/> /
/// <see cref="ChannelsEndpoints"/>). Agent create / edit / delete live in the
/// wizard (<c>/setup/agent</c>) — this surface is list-only.
/// </summary>
public static class AgentsEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps/{slug}").WithTags("agents").RequireAuthorization();

        group.MapGet("/agents", ListAgentsAsync)
            .WithName("agents.list")
            .Produces<AgentSummaryResponse[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
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

        // The agent config stores only the connection-string *name*, not the
        // model — resolve the model by joining against the database's AI
        // connection strings (one round trip, indexed by name).
        var connectionStrings = await maintenance.SendAsync(new GetConnectionStringsOperation(), ct);
        var modelByConnectionString = (connectionStrings.AiConnectionStrings ?? new Dictionary<string, AiConnectionString>())
            .ToDictionary(pair => pair.Key, pair => AiConnectionStringModel.Resolve(pair.Value), StringComparer.OrdinalIgnoreCase);

        // Usage (invocations + last-invoked) from the conversation index.
        var activity = await MetricsReadService.GetAgentActivityAsync(store, app.Database, ct);

        var items = (agents.AiAgents ?? [])
            .Select(agent =>
            {
                var (invocations, lastInvokedAt) = activity.TryGetValue(agent.Identifier, out var act)
                    ? act
                    : (0L, (DateTime?)null);
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
                    invocations,
                    SuccessRate: 0,
                    lastInvokedAt);
            })
            .OrderBy(agent => agent.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return Results.Ok(items);
    }
}
