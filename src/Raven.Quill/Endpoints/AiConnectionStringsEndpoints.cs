using System.Text.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;

namespace Raven.Quill.Endpoints;

public static class AiConnectionStringsEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/ai/connection-strings").RequireAuthorization();
        group.MapPost("/", PostAsync)
            .WithName("aiConnectionStrings.create")
            .Accepts<AiConnectionString>("application/json")
            .Produces<AiConnectionStringCreatedResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
        group.MapGet("/", ListAsync)
            .WithName("aiConnectionStrings.list")
            .Produces<List<AiConnectionString>>()
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

    public sealed class AiModelsRequest
    {
        public static AiModelsRequest From(AiConnectionString connection)
        {
            switch (connection.GetActiveProvider())
            {
                case AiConnectorType.OpenAi:
                    return new AiModelsRequest
                    {
                        ConnectorType = AiConnectorType.OpenAi,
                        OpenAiSettings = connection.OpenAiSettings
                    };
                case AiConnectorType.AzureOpenAi:
                    return new AiModelsRequest
                    {
                        ConnectorType = AiConnectorType.AzureOpenAi,
                        AzureOpenAiSettings = connection.AzureOpenAiSettings
                    };
                case AiConnectorType.Ollama:
                    return new AiModelsRequest
                    {
                        ConnectorType = AiConnectorType.Ollama,
                        OllamaSettings = connection.OllamaSettings
                    };
                case AiConnectorType.Google:
                    return new AiModelsRequest
                    {
                        ConnectorType = AiConnectorType.Google,
                        GoogleSettings = connection.GoogleSettings
                    };
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public AiConnectorType ConnectorType { get; set; }

        public OllamaSettings? OllamaSettings { get; set; }

        public OpenAiSettings? OpenAiSettings { get; set; }

        public AzureOpenAiSettings? AzureOpenAiSettings { get; set; }

        public GoogleSettings? GoogleSettings { get; set; }
    }

    private static async Task<IResult> DeleteAsync(
        string name,
        IDocumentStore store,
        ILogger<AiConnectionStringsLogger> logger,
        CancellationToken ct)
    {

        var existing = await store.Maintenance.Server
            .SendAsync(new GetServerWideConnectionStringsOperation(name, ConnectionStringType.Ai), ct);

        if (existing is null || existing.Results.Count == 0)
            return Results.NotFound(new ApiErrorResponse($"connection string '{name}' not found"));

        var usedBy = existing.Results.Single().UsedBy;

        // block delete: an agent still references this CS (would orphan it)
        if (usedBy.Count > 0)
        {
            return Results.Conflict(new AiConnectionStringDeleteConflictResponse(
                $"connection string '{name}' is referenced by agent(s); remove them first",
                usedBy.Select(used => $"App:{used.DatabaseName}, ID: {used.Identifier}, Kind: {used.Kind}").ToArray()));
        }

        await store.Maintenance.Server
            .SendAsync(new RemoveServerWideConnectionStringOperation<AiConnectionString>(new AiConnectionString { Name = name }), ct);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Deleted AI connection string name={Name}", name);

        return Results.NoContent();
    }

    private static async Task<IResult> GetByNameAsync(
        string name,
        IDocumentStore store,
        CancellationToken ct)
    {
        var result = await store.Maintenance.Server
            .SendAsync(new GetServerWideConnectionStringsOperation(name, ConnectionStringType.Ai), ct);

        if (result is null || result.Results.Count == 0)
            return Results.NotFound(new ApiErrorResponse($"connection string '{name}' not found"));

        var connectionString = result.Results.SingleOrDefault()?.ConnectionString as AiConnectionString;
        if (connectionString is null)
            return Results.NotFound(new ApiErrorResponse($"connection string '{name}' not found"));


        return Results.Ok(connectionString);
    }

    private static async Task<IResult> ListAsync(
        IDocumentStore store,
        CancellationToken ct)
    {
        var r = await store.Maintenance.Server
            .SendAsync(new GetServerWideConnectionStringsOperation(), ct);

        var results = r.Results.Select(c => c.ConnectionString).OfType<AiConnectionString>().ToList();
        return Results.Ok(results);
    }

    private static async Task<IResult> PostAsync(
        AiConnectionString body,
        IDocumentStore store,
        ILogger<AiConnectionStringsLogger> logger,
        CancellationToken ct)
    {
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
        switch (provider)
        {
            case AiConnectorType.OpenAi:
            case AiConnectorType.AzureOpenAi:
            case AiConnectorType.Ollama:
                // supported providers
                break;
            default:
                return Results.BadRequest(new ApiErrorResponse($"unsupported provider '{provider}'"));
        }

        var serverWideConnection = new ServerWideConnectionString
        {
            ConnectionString = connectionString,
        };

        await store.Maintenance.Server
            .SendAsync(new PutServerWideConnectionStringOperation(serverWideConnection), ct);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation(
                "Created AI connection string name={Name} provider={Provider}", connectionString.Name, connectionString.GetActiveProvider());

        return Results.Ok(new AiConnectionStringCreatedResponse(connectionString.Name));
    }

    internal sealed class AiConnectionStringsLogger;
}
