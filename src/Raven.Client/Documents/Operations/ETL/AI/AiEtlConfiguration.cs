using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class AiEtlConfiguration : EtlConfiguration<AiConnectionString>
{
    private string _name;
    private string Identifier => _name ??= Connection?.Name;

    public AiConnectorType AiConnectorType { get; set; }

    public List<string> PathsToProcess { get; set; }

    public override string GetDestination() => Identifier;
    public override string GetDefaultTaskName() => Identifier;

    public override EtlType EtlType => EtlType.Ai;

    public override bool UsingEncryptedCommunicationChannel()
    {
        switch (AiConnectorType)
        {
            case AiConnectorType.Ollama:
                return Connection.OllamaSettings.Uri.StartsWith("https");
            case AiConnectorType.OpenAi:
                return Connection.OpenAiSettings.Endpoint.StartsWith("https");
            case AiConnectorType.AzureOpenAi:
                return Connection.AzureOpenAiSettings.Endpoint.StartsWith("https");
            case AiConnectorType.MistralAi:
                return Connection.MistralAiSettings.Endpoint.StartsWith("https");
            case AiConnectorType.HuggingFace:
                // Endpoint is optional for HuggingFace, it will use the default endpoint if not provided, which is HTTPS
                return string.IsNullOrWhiteSpace(Connection.HuggingFaceSettings.Endpoint) || Connection.HuggingFaceSettings.Endpoint.StartsWith("https");
            case AiConnectorType.Onnx:
            case AiConnectorType.Google:
                return true;

            default:
                throw new NotSupportedException($"Unknown AI connector type: {AiConnectorType}");
        }
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(PathsToProcess)] = new DynamicJsonArray(PathsToProcess);
        json[nameof(AiConnectorType)] = AiConnectorType;

        return json;
    }
}
