using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Raven.AiAppliance.Contracts;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;

namespace Raven.AiAppliance.Endpoints;

public static class AiConnectionStringsEndpoints
{
    private static readonly JsonSerializerOptions DetailResponseJsonOptions = new(JsonSerializerDefaults.Web)
    {
        IgnoreReadOnlyProperties = true,
    };

    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps/{slug}/ai/connection-strings");
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
        App? app;
        using (var session = store.OpenAsyncSession())
        {
            app = await session.LoadAsync<App>($"apps/{slug}", ct);
        }

        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var existing = await store.Maintenance.ForDatabase(app.Database)
            .SendAsync(new GetConnectionStringsOperation(name, ConnectionStringType.Ai), ct);

        if (existing.AiConnectionStrings is null || existing.AiConnectionStrings.ContainsKey(name) == false)
            return Results.NotFound(new ApiErrorResponse($"connection string '{name}' not found"));

        // Reference check: refuse to delete if an agent on this DB still
        // references the CS. Without this, deletion would orphan the agent —
        // it would still exist but route requests to a no-longer-resolvable
        // connection string. Surface the offending agent identifier(s) so the
        // dashboard can render "remove agent(s) first" with a clickable list.
        var agents = await store.Maintenance.ForDatabase(app.Database)
            .SendAsync(new GetAiAgentsOperation(), ct);

        var referencing = (agents.AiAgents ?? [])
            .Where(a => string.Equals(a.ConnectionStringName, name, StringComparison.Ordinal))
            .Select(a => a.Identifier)
            .ToArray();

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
        App? app;
        using (var session = store.OpenAsyncSession())
        {
            app = await session.LoadAsync<App>($"apps/{slug}", ct);
        }

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
        App? app;
        using (var session = store.OpenAsyncSession())
        {
            app = await session.LoadAsync<App>($"apps/{slug}", ct);
        }

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
        App? app;
        using (var session = store.OpenAsyncSession())
        {
            app = await session.LoadAsync<App>($"apps/{slug}", ct);
        }

        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        if (body is null || string.IsNullOrWhiteSpace(body.Name))
            return Results.BadRequest(new ApiErrorResponse("name is required"));

        var connectionString = body;

        // ConnectionString.Validate() runs AiConnectionString.ValidateImpl —
        // the "exactly one provider" rule plus each provider's ValidateFields
        // (empty ApiKey, empty Model, empty Ollama URI, etc). Surface those at
        // intake as 400 instead of bubbling up RavenDB's 500 from the PUT.
        var errors = new List<string>();
        if (connectionString.Validate(errors) == false)
            return Results.BadRequest(new ApiErrorResponse(string.Join("; ", errors)));

        // Agent provisioning needs a Chat model. Embeddings/TextEmbeddings will
        // arrive with its own future endpoint; gate so an operator doesn't
        // wire a chat-only agent to an embeddings-only connection string.
        if (connectionString.ModelType != AiModelType.Chat)
            return Results.BadRequest(new ApiErrorResponse($"AI agent connection strings require ModelType=Chat; got '{connectionString.ModelType}'"));

        // Demo gate: only OpenAi + Ollama are smoke-tested end-to-end in the
        // 8-week scope. The other RavenDB providers (Azure, Google, HuggingFace,
        // Mistral, Vertex, Embedded) work upstream but we haven't verified the
        // appliance plumbing for them. One-line lift once each is smoked.
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
