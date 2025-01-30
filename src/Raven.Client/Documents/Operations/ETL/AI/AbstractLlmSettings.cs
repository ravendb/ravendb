using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.AI;

public abstract class AbstractAiSettings : IDynamicJsonValueConvertible
{
    public abstract bool HasSettings();
    public abstract AiSettingsCompareDifferences Compare(AbstractAiSettings other);

    public abstract DynamicJsonValue ToJson();
}

[Flags]
public enum AiSettingsCompareDifferences
{
    None = 0,

    // Changes that affect the mathematical structure of embeddings
    EmbeddingDimensions = 1 << 0,
    EmbeddingNormalization = 1 << 1,
    PoolingStrategy = 1 << 2,
    ModelArchitecture = 1 << 3,         // Changes in model name/version that affect embedding structure

    // Changes in text preprocessing that affect input
    TextPreprocessing = 1 << 4,         // e.g. case sensitivity, unicode normalization
    TokenizationSettings = 1 << 5,      // e.g. special tokens (CLS, SEP, PAD, etc.)
    SequenceLimits = 1 << 6,            // e.g. maximum tokens

    // Changes in API configuration
    EndpointConfiguration = 1 << 7,     // Changes in endpoint URLs
    AuthenticationSettings = 1 << 8,    // Changes in API keys, org IDs etc

    // Changes that could affect embedding generation but cannot be verified by comparing settings
    DeploymentConfiguration = 1 << 9,

    // Combinations for common scenarios
    EmbeddingStructure = EmbeddingDimensions | EmbeddingNormalization | PoolingStrategy | ModelArchitecture,
    InputProcessing = TextPreprocessing | TokenizationSettings | SequenceLimits,
    ConnectionConfig = EndpointConfiguration | AuthenticationSettings,

    RequiresEmbeddingsRegeneration = EmbeddingStructure | InputProcessing | DeploymentConfiguration,

    // All changes
    All = RequiresEmbeddingsRegeneration | ConnectionConfig
}
