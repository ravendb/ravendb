using Raven.Client.Documents.Operations.AI;

namespace Raven.Quill.Endpoints.Helpers;

internal static class AiConnectionStringModel
{
    public static string? Resolve(AiConnectionString cs) =>
        cs.OpenAiSettings?.Model
        ?? cs.AzureOpenAiSettings?.Model
        ?? cs.OllamaSettings?.Model
        ?? cs.GoogleSettings?.Model
        ?? cs.HuggingFaceSettings?.Model
        ?? cs.MistralAiSettings?.Model
        ?? cs.VertexSettings?.Model;
}
