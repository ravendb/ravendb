using System.Text.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Logging;
using Raven.Server.Logging;

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
        group.MapPost("/test", TestAsync)
            .WithName("aiConnectionStrings.test")
            .Accepts<AiConnectionString>("application/json")
            .Produces<AiConnectionStringTestResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
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

    private static async Task<IResult> DeleteAsync(
        string name,
        IDocumentStore store,
        QuillLogger<AiConnectionStringsLogger> logger,
        HttpContext ctx,
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

        if (logger.IsInfoEnabled)
            logger.Info($"Deleted AI connection string name={name}");

        if (logger.AuditEnabled)
            logger.Audit("DELETE", $"AiConnectionString '{name}'", ctx);

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
        QuillLogger<AiConnectionStringsLogger> logger,
        HttpContext ctx,
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
        if (IsSupportedProvider(provider) == false)
            return Results.BadRequest(new ApiErrorResponse($"unsupported provider '{provider}'"));

        var serverWideConnection = new ServerWideConnectionString
        {
            ConnectionString = connectionString,
        };

        await store.Maintenance.Server
            .SendAsync(new PutServerWideConnectionStringOperation(serverWideConnection), ct);

        if (logger.IsInfoEnabled)
            logger.Info(
                $"Created AI connection string name={connectionString.Name} " +
                $"provider={connectionString.GetActiveProvider()}");

        if (logger.AuditEnabled)
            logger.Audit("POST",
                $"AiConnectionString '{connectionString.Name}' provider={provider} modelType={connectionString.ModelType}",
                ctx);

        return Results.Ok(new AiConnectionStringCreatedResponse(connectionString.Name));
    }

    private static async Task<IResult> TestAsync(
        AiConnectionString body,
        IAiHelperClient aiClient,
        QuillLogger<AiConnectionStringsLogger> logger,
        HttpContext ctx,
        CancellationToken ct)
    {
        var errors = new List<string>();
        if (body.Validate(errors) == false)
            return Results.BadRequest(new ApiErrorResponse(string.Join("; ", errors)));

        if (body.ModelType != AiModelType.Chat)
            return Results.BadRequest(new ApiErrorResponse($"AI agent connection strings require ModelType=Chat; got '{body.ModelType}'"));

        var provider = body.GetActiveProvider();
        if (IsSupportedProvider(provider) == false)
            return Results.BadRequest(new ApiErrorResponse($"unsupported provider '{provider}'"));

        var path = $"{TestConnectionPath}?type={provider}&modelType={AiModelType.Chat}";
        var (transport, content) = await aiClient.SendAsync(path, "POST", GetProviderSettings(body, provider), ct);

        AiConnectionStringTestResponse response;
        if (transport != AiHelperStatus.Success)
        {
            if (logger.IsWarnEnabled)
                logger.Warn(
                    $"Could not run the AI model test for connection string name={body.Name}: " +
                    $"transport {transport}.");
            response = new AiConnectionStringTestResponse(
                Success: false, Error: "Could not reach the provider to verify the model.");
        }
        else
        {
            var result = await aiClient.DeserializeAsync<AiTestConnectionResult>(content, ct);
            response = result switch
            {
                null => new AiConnectionStringTestResponse(
                    Success: false, Error: "Could not read the model test result."),
                { Success: false } => new AiConnectionStringTestResponse(Success: false, Error: result.Error),
                { SupportsTools: false } => new AiConnectionStringTestResponse(
                    Success: false,
                    Error: $"Model '{GetModelName(body, provider)}' does not support function tools, so it can't be used by an agent. Pick a different model."),
                _ => new AiConnectionStringTestResponse(Success: true),
            };
        }

        if (logger.AuditEnabled)
            logger.Audit("POST",
                $"AiConnectionString '{body.Name}' tested provider={provider} success={response.Success}", ctx);

        return Results.Ok(response);
    }

    private static object GetProviderSettings(AiConnectionString connection, AiConnectorType provider) =>
        provider switch
        {
            AiConnectorType.OpenAi => connection.OpenAiSettings,
            AiConnectorType.AzureOpenAi => connection.AzureOpenAiSettings,
            AiConnectorType.Ollama => connection.OllamaSettings,
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, "unsupported provider"),
        };

    private static string? GetModelName(AiConnectionString connection, AiConnectorType provider) =>
        provider switch
        {
            AiConnectorType.OpenAi => connection.OpenAiSettings?.Model,
            AiConnectorType.AzureOpenAi => connection.AzureOpenAiSettings?.DeploymentName,
            AiConnectorType.Ollama => connection.OllamaSettings?.Model,
            _ => null,
        };

    private static bool IsSupportedProvider(AiConnectorType provider) =>
        provider is AiConnectorType.OpenAi or AiConnectorType.AzureOpenAi or AiConnectorType.Ollama;

    private const string TestConnectionPath = "/admin/ai/test-connection";

    private sealed class AiTestConnectionResult
    {
        public bool Success { get; set; }
        public string? Error { get; set; }
        public bool SupportsTools { get; set; }
    }

    private sealed class AiConnectionStringsLogger;
}
