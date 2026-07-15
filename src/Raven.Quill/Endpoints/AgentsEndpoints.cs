using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Metrics;

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

        var connectionStrings = await maintenance.SendAsync(new GetConnectionStringsOperation(), ct);
        var modelByConnectionString = (connectionStrings.AiConnectionStrings ?? new Dictionary<string, AiConnectionString>())
            .ToDictionary(pair => pair.Key, pair => AiConnectionStringModel.Resolve(pair.Value), StringComparer.OrdinalIgnoreCase);

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
}
