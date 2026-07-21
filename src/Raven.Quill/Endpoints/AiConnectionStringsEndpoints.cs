using System.Text.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;

namespace Raven.Quill.Endpoints;

public static class AiConnectionStringsEndpoints
{
    private static readonly JsonSerializerOptions DetailResponseJsonOptions = new(JsonSerializerDefaults.Web)
    {
        IgnoreReadOnlyProperties = true,
    };

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps/{slug}/ai/connection-strings").RequireAuthorization();
        group.MapPost("/", PostAsync)
            .WithName("aiConnectionStrings.create")
            .Accepts<AiConnectionString>("application/json")
            .Produces<AiConnectionStringCreatedResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
        group.MapGet("/", ListAsync)
            .WithName("aiConnectionStrings.list")
            .Produces<AiConnectionStringListResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
        group.MapGet("/{name}", GetByNameAsync)
            .WithName("aiConnectionStrings.detail")
            .Produces<AiConnectionString>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
        group.MapDelete("/{name}", DeleteAsync)
            .WithName("aiConnectionStrings.delete")
            .Produces(StatusCodes.Status204NoContent)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound)
            .Produces<AiConnectionStringDeleteConflictResponse>(StatusCodes.Status409Conflict);
    }

    private static async Task<IResult> DeleteAsync(
        string slug,
        string name,
        IDocumentStore store,
        ILogger<AiConnectionStringsLogger> logger,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);

        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var existing = await store.Maintenance.ForDatabase(app.Database)
            .SendAsync(new GetConnectionStringsOperation(name, ConnectionStringType.Ai), ct);

        if (existing.AiConnectionStrings is null || existing.AiConnectionStrings.ContainsKey(name) == false)
            return Results.NotFound(new ApiErrorResponse($"connection string '{name}' not found"));

        var agents = await store.Maintenance.ForDatabase(app.Database)
            .SendAsync(new GetAiAgentsOperation(), ct);

        var referencing = (agents.AiAgents ?? [])
            .Where(a => string.Equals(a.ConnectionStringName, name, StringComparison.Ordinal))
            .Select(a => a.Identifier)
            .ToArray();

        // block delete: an agent still references this CS (would orphan it)
        if (referencing.Length > 0)
        {
            return Results.Conflict(new AiConnectionStringDeleteConflictResponse(
                $"connection string '{name}' is referenced by agent(s); remove them first",
                referencing));
        }

        await store.Maintenance.ForDatabase(app.Database)
            .SendAsync(new RemoveConnectionStringOperation<AiConnectionString>(new AiConnectionString { Name = name }), ct);

        logger.LogInformation("Deleted AI connection string slug={Slug} name={Name}", app.Slug, name);
        return Results.NoContent();
    }

    private static async Task<IResult> GetByNameAsync(
        string slug,
        string name,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);

        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var result = await store.Maintenance.ForDatabase(app.Database)
            .SendAsync(new GetConnectionStringsOperation(name, ConnectionStringType.Ai), ct);

        if (result.AiConnectionStrings is null || result.AiConnectionStrings.TryGetValue(name, out var cs) == false)
            return Results.NotFound(new ApiErrorResponse($"connection string '{name}' not found"));

        return Results.Json(cs, DetailResponseJsonOptions);
    }

    private static async Task<IResult> ListAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);

        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var result = await store.Maintenance.ForDatabase(app.Database)
            .SendAsync(new GetConnectionStringsOperation(), ct);

        var items = (result.AiConnectionStrings ?? new Dictionary<string, AiConnectionString>())
            .Values
            .Select(cs => new AiConnectionStringListItemResponse(
                cs.Name,
                cs.Identifier,
                cs.ModelType,
                cs.GetActiveProvider()))
            .ToArray();

        return Results.Ok(new AiConnectionStringListResponse(items));
    }

    private static async Task<IResult> PostAsync(
        string slug,
        AiConnectionString body,
        IDocumentStore store,
        ILogger<AiConnectionStringsLogger> logger,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);

        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        if (body is null || string.IsNullOrWhiteSpace(body.Name))
            return Results.BadRequest(new ApiErrorResponse("name is required"));

        var connectionString = body;

        var errors = new List<string>();
        if (connectionString.Validate(errors) == false)
            return Results.BadRequest(new ApiErrorResponse(string.Join("; ", errors)));

        // require a Chat model; an embeddings-only CS can't back an agent
        if (connectionString.ModelType != AiModelType.Chat)
            return Results.BadRequest(new ApiErrorResponse($"AI agent connection strings require ModelType=Chat; got '{connectionString.ModelType}'"));

        var provider = connectionString.GetActiveProvider();
        if (provider != AiConnectorType.OpenAi && provider != AiConnectorType.Ollama)
            return Results.BadRequest(new ApiErrorResponse($"unsupported provider '{provider}' in demo; supported: OpenAi, Ollama"));

        await store.Maintenance.ForDatabase(app.Database)
            .SendAsync(new PutConnectionStringOperation<AiConnectionString>(connectionString), ct);

        logger.LogInformation(
            "Created AI connection string slug={Slug} name={Name} provider={Provider}",
            app.Slug, connectionString.Name, connectionString.GetActiveProvider());

        return Results.Ok(new AiConnectionStringCreatedResponse(connectionString.Name));
    }

    internal sealed class AiConnectionStringsLogger;
}
