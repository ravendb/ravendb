using System.Text.Json;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Client.Documents.Operations.AI;

namespace Raven.Quill.Endpoints;

public static class AiModelsEndpoints
{
    private const string ModelsPath = "/studio-tasks/ai/models";

    private static readonly JsonSerializerOptions EnvelopeJsonOptions = new(JsonSerializerDefaults.Web);

    public static void Map(WebApplication app)
    {
        app.MapPost("/api/ai/models", PostAsync)
            .RequireAuthorization()
            .WithName("aiModels.list")
            .Produces<AiModelsResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status502BadGateway);
    }

    private static async Task<IResult> PostAsync(
        AiModelsRequest body,
        IAiHelperClient ravendb,
        CancellationToken ct)
    {
        object? settings = body.ConnectorType switch
        {
            AiConnectorType.OpenAi => body.OpenAiSettings,
            AiConnectorType.AzureOpenAi => body.AzureOpenAiSettings,
            AiConnectorType.Ollama => body.OllamaSettings,
            AiConnectorType.Google => body.GoogleSettings,
            _ => null,
        };

        if (settings is null)
        {
            return Results.BadRequest(new ApiErrorResponse(
                $"connector '{body.ConnectorType}' with its settings is required; supported connectors: OpenAi, AzureOpenAi, Ollama, Google"));
        }

        var (transport, content) = await ravendb.SendAsync(ModelsPath, "POST", body, ct);
        if (transport != AiHelperStatus.Success)
        {
            return Results.Json(
                new ApiErrorResponse("failed to fetch models from the provider"),
                statusCode: StatusCodes.Status502BadGateway);
        }

        var models = ParseModelIds(content);
        return models is null
            ? Results.Json(
                new ApiErrorResponse("unexpected model-list response from the provider"),
                statusCode: StatusCodes.Status502BadGateway)
            : Results.Ok(new AiModelsResponse(models));
    }

    private static string[]? ParseModelIds(string content)
    {
        try
        {
            var envelope = JsonSerializer.Deserialize<ModelsEnvelope>(content, EnvelopeJsonOptions);
            return envelope?.Data?
                .Select(model => model.Id)
                .Where(id => string.IsNullOrWhiteSpace(id) == false)
                .Select(id => id!)
                .ToArray();
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private sealed class ModelsEnvelope
    {
        public ModelEntry[]? Data { get; set; }
    }

    private sealed class ModelEntry
    {
        public string? Id { get; set; }
    }
}
