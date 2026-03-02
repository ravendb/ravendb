using System;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Flags describing what differs between two AI settings/configurations.
/// These distinctions help determine whether embeddings must be regenerated
/// or if a simple configuration update is sufficient.
/// </summary>
[Flags]
public enum AiSettingsCompareDifferences
{
    /// <summary>No differences.</summary>
    None = 0,

    /// <summary>Identifier value changed.</summary>
    Identifier = 1 << 0,

    /// <summary>Embedding dimensionality changed.</summary>
    EmbeddingDimensions = 1 << 1,
    /// <summary>Model name/version changed in a way that affects embedding structure.</summary>
    ModelArchitecture = 1 << 2,

    /// <summary>Endpoint/URL configuration changed.</summary>
    EndpointConfiguration = 1 << 3,

    /// <summary>Authentication-related settings changed (API key, org/project, etc.).</summary>
    AuthenticationSettings = 1 << 4,

    /// <summary>Provider-specific deployment changed (e.g., Azure deployment name).</summary>
    DeploymentConfiguration = 1 << 5,

    /// <summary>Convenience combination representing structure-impacting changes.</summary>
    EmbeddingStructure = Identifier | EmbeddingDimensions | ModelArchitecture,
    /// <summary>Convenience combination representing connection-related changes.</summary>
    ConnectionConfig = EndpointConfiguration | AuthenticationSettings,

    /// <summary>Indicates that previously generated embeddings should be regenerated.</summary>
    RequiresEmbeddingsRegeneration = EmbeddingStructure | DeploymentConfiguration,

    /// <summary>All changes.</summary>
    All = RequiresEmbeddingsRegeneration | ConnectionConfig
}
