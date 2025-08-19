using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// Base class for AI-related ETL configurations (e.g., embeddings generation, GenAI processing).
/// Provides common behaviors and access to the configured AI provider type.
/// </summary>
public abstract class AbstractAiIntegrationConfiguration : EtlConfiguration<AiConnectionString>
{
    /// <summary>
    /// Returns the active AI connector type derived from the associated connection string,
    /// or <see cref="AiConnectorType.None"/> if no provider is configured.
    /// </summary>
    [JsonDeserializationIgnore]
    [JsonIgnore]
    public AiConnectorType AiConnectorType => Connection?.GetActiveProvider() ?? AiConnectorType.None;
}
