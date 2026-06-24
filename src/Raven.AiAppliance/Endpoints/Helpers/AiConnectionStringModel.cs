using Raven.Client.Documents.Operations.AI;

namespace Raven.AiAppliance.Endpoints.Helpers;

internal static class AiConnectionStringModel
{
    /// <summary>The chat model lives on whichever provider settings the connection
    /// string has set (the "exactly one provider" rule means at most one is
    /// non-null). Embedded (ONNX) settings carry no model.</summary>
    public static string? Resolve(AiConnectionString cs) =>
        cs.OpenAiSettings?.Model
        ?? cs.AzureOpenAiSettings?.Model
        ?? cs.OllamaSettings?.Model
        ?? cs.GoogleSettings?.Model
        ?? cs.HuggingFaceSettings?.Model
        ?? cs.MistralAiSettings?.Model
        ?? cs.VertexSettings?.Model;
}
