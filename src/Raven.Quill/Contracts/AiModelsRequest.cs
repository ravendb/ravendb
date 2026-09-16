using Raven.Client.Documents.Operations.AI;

namespace Raven.Quill.Contracts;

public sealed class AiModelsRequest
{
    public AiConnectorType ConnectorType { get; set; }

    public OpenAiSettings? OpenAiSettings { get; set; }

    public AzureOpenAiSettings? AzureOpenAiSettings { get; set; }

    public OllamaSettings? OllamaSettings { get; set; }

    public GoogleSettings? GoogleSettings { get; set; }
}
