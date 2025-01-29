using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class AiEtlConfiguration : EtlConfiguration<AiConnectionString>
{
    private string _name;
    private string Identifier => _name ??= Connection?.Name;

    public AiConnectorType AiConnectorType { get; set; }

    public List<string> FieldsToInclude { get; set; }

    public override string GetDestination() => Identifier;
    public override string GetDefaultTaskName() => Identifier;

    public override EtlType EtlType => EtlType.Ai;

    public override bool UsingEncryptedCommunicationChannel()
    {
        switch (AiConnectorType)
        {
            case AiConnectorType.Ollama:
                return Connection.OllamaSettings.Uri.StartsWith("https");

            // todo: other AI connectors

            default:
                throw new NotSupportedException($"Unknown AI connector type: {AiConnectorType}");
        }

        return false;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(FieldsToInclude)] = new DynamicJsonArray(FieldsToInclude);
        json[nameof(AiConnectorType)] = AiConnectorType;

        return json;
    }
}
