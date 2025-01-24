using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class VectorEmbeddingEnrichmentEtlConfiguration : EtlConfiguration<AiEtlConnectionString>
{
    private string _name;
    private string Identifier => _name ??= Connection?.Name;

    public LlmProviderType LlmProviderType { get; set; }

    public List<string> FieldsToInclude { get; set; }

    public override string GetDestination() => Identifier;
    public override string GetDefaultTaskName() => Identifier;

    public override EtlType EtlType => EtlType.VectorEmbeddingEnrichment;

    public override bool UsingEncryptedCommunicationChannel()
    {
        switch (LlmProviderType)
        {
            case LlmProviderType.Ollama:
                return Connection.OllamaSettings.Uri.StartsWith("https");
            default:
                throw new NotSupportedException($"Unknown LLM provider type: {LlmProviderType}");
        }

        return false;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(FieldsToInclude)] = new DynamicJsonArray(FieldsToInclude);
        json[nameof(LlmProviderType)] = LlmProviderType;

        return json;
    }
}
