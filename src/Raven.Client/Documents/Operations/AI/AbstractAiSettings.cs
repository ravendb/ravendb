using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public abstract class AbstractAiSettings : IDynamicJsonValueConvertible
{
    public abstract void ValidateMandatoryFields(ref List<string> errors);
    public abstract AiSettingsCompareDifferences Compare(AbstractAiSettings other);

    public abstract DynamicJsonValue ToJson();
}

[Flags]
public enum AiSettingsCompareDifferences
{
    None = 0,

    Identifier = 1 << 0,

    // Changes that affect the mathematical structure of embeddings
    EmbeddingDimensions = 1 << 1,
    ModelArchitecture = 1 << 2,         // Changes in model name/version that affect embedding structure

    // Changes in API configuration
    EndpointConfiguration = 1 << 3,     // Changes in endpoint URLs
    AuthenticationSettings = 1 << 4,    // Changes in API keys, org IDs etc

    // Changes that could affect embedding generation but cannot be verified by comparing settings
    DeploymentConfiguration = 1 << 5,

    // Combinations for common scenarios
    EmbeddingStructure = Identifier | EmbeddingDimensions | ModelArchitecture,
    ConnectionConfig = EndpointConfiguration | AuthenticationSettings,

    RequiresEmbeddingsRegeneration = EmbeddingStructure | DeploymentConfiguration,

    // All changes
    All = RequiresEmbeddingsRegeneration | ConnectionConfig
}
