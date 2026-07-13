using Raven.Client.Documents.Operations.AI;

namespace Raven.Quill.Contracts;

/// <summary>
/// Mirrors the request contract of RavenDB's <c>/studio-tasks/ai/models</c> endpoint:
/// the connector type plus the settings object for that connector (only the matching
/// one needs to be set). Other connectors don't support model listing upstream.
/// </summary>
public sealed class AiModelsRequest
{
    public AiConnectorType ConnectorType { get; set; }

    public OpenAiSettings? OpenAiSettings { get; set; }

    public AzureOpenAiSettings? AzureOpenAiSettings { get; set; }

    public OllamaSettings? OllamaSettings { get; set; }

    public GoogleSettings? GoogleSettings { get; set; }
}
